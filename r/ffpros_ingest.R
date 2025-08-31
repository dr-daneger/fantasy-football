#!/usr/bin/env Rscript
# ffpros_ingest.R — FP weekly projections + weekly ECR dispersion + ROS rankings
# Writes: data/staging/fp_weekly.parquet, fp_ros.parquet (+ CSV best effort)

# ---------- bootstrap ----------
pkgs <- c("ffpros", "dplyr", "tidyr", "stringr", "arrow")
for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) {
    cat("[Bootstrap] Installing ", p, "...\n", sep = "")
    repos <- if (p == "ffpros")
      c("https://ffverse.r-universe.dev", "https://cloud.r-project.org")
    else
      "https://cloud.r-project.org"
    install.packages(p, repos = repos)
  }
}
suppressPackageStartupMessages({
  library(ffpros)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(arrow)
})

# ---------- config ----------
YEAR    <- as.integer(Sys.getenv("FP_YEAR", "2025"))
WEEK    <- as.integer(Sys.getenv("FP_WEEK", "1"))
SCORING <- Sys.getenv("FP_SCORING", "PPR")
PAGES   <- strsplit(Sys.getenv("FP_POS", "qb,rb,wr,te,k,dst"), "\\s*,\\s*")[[1]]
PAGES   <- tolower(unique(PAGES))

STAGING_DIR <- "data/staging"
dir.create(STAGING_DIR, recursive = TRUE, showWarnings = FALSE)

pulled_at_utc <- format(as.POSIXct(Sys.time(), tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ")

# ---------- helpers ----------
as_chr_safe <- function(x) {
  to_one <- function(el) {
    if (is.null(el) || length(el) == 0) return(NA_character_)
    as.character(el[[1]])
  }
  out <- if (is.list(x)) vapply(x, to_one, FUN.VALUE = character(1L)) else as.character(x)
  out[!is.na(out) & grepl("^\\s*character\\(0\\)\\s*$", out, ignore.case = TRUE)] <- NA_character_
  out
}

slugify <- function(x) {
  x <- as_chr_safe(x)
  x <- iconv(x, to = "ASCII//TRANSLIT")
  x <- tolower(x)
  x <- gsub("\\b(jr|sr|ii|iii|iv|v)\\b\\.?", "", x, ignore.case = TRUE)
  x <- gsub("\\s+", " ", x)
  x <- gsub("[^a-z0-9]+", "-", x)
  x <- gsub("(^-|-$)", "", x)
  x
}

pick_col_regex <- function(nm, patterns) {
  for (p in patterns) {
    hit <- nm[str_detect(nm, regex(p, ignore_case = TRUE))]
    if (length(hit) > 0) return(hit[1])
  }
  NULL
}

pick_any <- function(nm, ...) {
  pats <- c(...)
  for (p in pats) {
    hit <- nm[str_detect(nm, regex(p, ignore_case = TRUE))]
    if (length(hit) > 0) return(hit[1])
  }
  NULL
}

numify <- function(x) {
  suppressWarnings(as.numeric(gsub(",", "", as_chr_safe(x))))
}

safe_write_csv <- function(df, path) {
  tryCatch({
    if (file.exists(path)) {
      tmp <- paste0(path, ".tmp")
      utils::write.csv(df, tmp, row.names = FALSE)
      file.rename(tmp, path)
    } else {
      utils::write.csv(df, path, row.names = FALSE)
    }
  }, error = function(e) {
    message(sprintf("[ffpros][warn] Could not write %s (%s). Continuing.", path, e$message))
  })
}

# ---------- normalize: weekly projections ----------
norm_weekly_proj <- function(df, pos) {
  if (nrow(df) == 0) return(df)

  nm <- names(df)
  player_col <- pick_col_regex(nm, c("^player$", "player_name", "^name$"))
  team_col   <- pick_col_regex(nm, c("^team$", "nfl[_ ]?team", "^tm$"))
  id_col     <- pick_any(nm, "fantasypros_?id", "^fp_?id$", "player_?id$")
  pts_col    <- pick_col_regex(
    nm,
    c("fpts_ppr", "misc_fpts", "ppr.*pts", "points.*ppr", "fantasy.?points?.*ppr",
      "^fpts$", "fantasy.?points?", "proj.?pts?")
  )
  wk_sd_col  <- pick_any(nm, "^sd$", "stdev", "std.?dev")
  wk_min_col <- pick_any(nm, "^min$", "floor", "low", "lower")
  wk_max_col <- pick_any(nm, "^max$", "ceiling", "high", "upper")

  if (is.null(player_col)) player_col <- nm[1]
  if (is.null(team_col))   team_col   <- if (length(nm) >= 2) nm[2] else nm[1]

  message(sprintf(
    "[map][%s/Stats] player=%s team=%s id=%s pts=%s",
    toupper(pos), player_col, team_col,
    ifelse(is.null(id_col), "None", id_col),
    ifelse(is.null(pts_col), "None", pts_col)
  ))

  out <- df %>%
    mutate(
      pos = toupper(pos),
      year = YEAR,
      week = WEEK,
      pulled_at = pulled_at_utc,
      source = "fantasypros",
      player = as_chr_safe(.data[[player_col]]),
      team   = as_chr_safe(.data[[team_col]]),
      fp_player_id = if (!is.null(id_col)) suppressWarnings(as.integer(.data[[id_col]])) else NA_integer_,
      fantasypts   = if (!is.null(pts_col)) numify(.data[[pts_col]]) else NA_real_,
      weekly_sd    = if (!is.null(wk_sd_col))  numify(.data[[wk_sd_col]]) else NA_real_,
      weekly_floor = if (!is.null(wk_min_col)) numify(.data[[wk_min_col]]) else NA_real_,
      weekly_ceiling = if (!is.null(wk_max_col)) numify(.data[[wk_max_col]]) else NA_real_,
      fp_player_slug = slugify(.data[[player_col]])
    ) %>%
    mutate(
      `__fp_key` = paste0(
        fp_player_slug, "|", pos, "|",
        tolower(dplyr::if_else(is.na(team) | team == "", "", team))
      ),
      weekly_avg = dplyr::case_when(
        !is.na(fantasypts) ~ fantasypts,
        is.na(fantasypts) & !is.na(weekly_floor) & !is.na(weekly_ceiling) ~
          (weekly_floor + weekly_ceiling) / 2,
        TRUE ~ NA_real_
      )
    )

  out
}

# ---------- normalize: weekly ECR rankings ----------
norm_weekly_rank <- function(df, pos) {
  if (nrow(df) == 0) return(tibble())

  nm <- names(df)
  player_col <- pick_col_regex(nm, c("^player$", "player_name", "^name$"))
  team_col   <- pick_col_regex(nm, c("^team$", "nfl[_ ]?team", "^tm$"))
  id_col     <- pick_any(nm, "fantasypros_?id", "^fp_?id$", "player_?id$")
  rk_col     <- pick_col_regex(nm, c("^rank$", "^rk$", "ecr", "overall.?rank"))
  best_col   <- pick_any(nm, "^best$")
  worst_col  <- pick_any(nm, "^worst$")
  avg_col    <- pick_any(nm, "^avg\\.?$", "average")
  sd_col     <- pick_any(nm, "^sd$", "std\\.?dev")

  if (is.null(player_col)) player_col <- nm[1]
  if (is.null(team_col))   team_col   <- if (length(nm) >= 2) nm[2] else nm[1]

  message(sprintf(
    "[map][%s/Ranks] player=%s team=%s id=%s rk=%s avg=%s sd=%s",
    toupper(pos), player_col, team_col,
    ifelse(is.null(id_col), "None", id_col),
    ifelse(is.null(rk_col), "None", rk_col),
    ifelse(is.null(avg_col), "None", avg_col),
    ifelse(is.null(sd_col), "None", sd_col)
  ))

  out <- df %>%
    mutate(
      pos = toupper(pos),
      year = YEAR,
      week = WEEK,
      pulled_at = pulled_at_utc,
      source = "fantasypros",
      player = as_chr_safe(.data[[player_col]]),
      team   = as_chr_safe(.data[[team_col]]),
      fp_player_id = if (!is.null(id_col)) suppressWarnings(as.integer(.data[[id_col]])) else NA_integer_,
      weekly_ecr_rank       = if (!is.null(rk_col))  suppressWarnings(as.integer(.data[[rk_col]])) else NA_integer_,
      weekly_ecr_best_rank  = if (!is.null(best_col)) suppressWarnings(as.integer(.data[[best_col]])) else NA_integer_,
      weekly_ecr_worst_rank = if (!is.null(worst_col)) suppressWarnings(as.integer(.data[[worst_col]])) else NA_integer_,
      weekly_ecr_avg_rank   = if (!is.null(avg_col))  suppressWarnings(as.numeric(.data[[avg_col]])) else NA_real_,
      weekly_ecr_sd_rank    = if (!is.null(sd_col))   suppressWarnings(as.numeric(.data[[sd_col]])) else NA_real_,
      fp_player_slug = slugify(.data[[player_col]])
    ) %>%
    mutate(
      `__fp_key` = paste0(
        fp_player_slug, "|", pos, "|",
        tolower(dplyr::if_else(is.na(team) | team == "", "", team))
      )
    )

  out
}

# ---------- normalize: ROS (rankings or projections) ----------
norm_ros <- function(df, pos) {
  if (nrow(df) == 0) return(df)

  nm <- names(df)
  player_col <- pick_col_regex(nm, c("^player$", "player_name", "^name$"))
  team_col   <- pick_col_regex(nm, c("^team$", "nfl[_ ]?team", "^tm$"))
  id_col     <- pick_any(nm, "fantasypros_?id", "^fp_?id$", "player_?id$")
  pts_col    <- pick_col_regex(nm, c("^ros_?points?$", "fpts", "fantasy.?points?", "proj.?total"))
  rank_col   <- pick_col_regex(nm, c("^ros_?rank$", "^rank$", "ecr", "overall.?rank"))
  best_col   <- pick_any(nm, "^best$")
  worst_col  <- pick_any(nm, "^worst$")
  avg_col    <- pick_any(nm, "^avg\\.?$", "average")
  sd_col     <- pick_any(nm, "^sd$", "std\\.?dev")

  if (is.null(player_col)) player_col <- nm[1]
  if (is.null(team_col))   team_col   <- if (length(nm) >= 2) nm[2] else nm[1]

  out <- df %>%
    mutate(
      pos = toupper(pos),
      year = YEAR,
      pulled_at = pulled_at_utc,
      source = "fantasypros",
      player = as_chr_safe(.data[[player_col]]),
      team   = as_chr_safe(.data[[team_col]]),
      fp_player_id = if (!is.null(id_col)) suppressWarnings(as.integer(.data[[id_col]])) else NA_integer_,
      ros_points    = if (!is.null(pts_col))  numify(.data[[pts_col]]) else NA_real_,
      ros_rank      = if (!is.null(rank_col)) suppressWarnings(as.integer(.data[[rank_col]])) else NA_integer_,
      ros_best_rank = if (!is.null(best_col)) suppressWarnings(as.integer(.data[[best_col]])) else NA_integer_,
      ros_worst_rank= if (!is.null(worst_col)) suppressWarnings(as.integer(.data[[worst_col]])) else NA_integer_,
      ros_avg_rank  = if (!is.null(avg_col))  suppressWarnings(as.numeric(.data[[avg_col]])) else NA_real_,
      ros_sd_rank   = if (!is.null(sd_col))   suppressWarnings(as.numeric(.data[[sd_col]])) else NA_real_,
      fp_player_slug = slugify(.data[[player_col]])
    ) %>%
    mutate(
      `__fp_key` = paste0(
        fp_player_slug, "|", pos, "|",
        tolower(dplyr::if_else(is.na(team) | team == "", "", team))
      )
    )

  out
}

# ---------- pulls ----------
pull_weekly_for_pos <- function(pg) {
  message(sprintf("[ffpros] Weekly: %s (year=%d week=%d scoring=%s)", toupper(pg), YEAR, WEEK, SCORING))

  proj <- tryCatch(
    fp_projections(page = pg, year = YEAR, week = WEEK, scoring = SCORING),
    error = function(e) { warning(sprintf("Weekly projections failed for %s: %s", pg, e$message)); tibble() }
  )
  ranks <- tryCatch(
    fp_rankings(page = pg, year = YEAR, week = WEEK),
    error = function(e) { message(sprintf("Weekly rankings not available for %s: %s", pg, e$message)); tibble() }
  )

  wp <- norm_weekly_proj(proj, pg)
  wr <- norm_weekly_rank(ranks, pg)

  if (nrow(wp) > 0 && nrow(wr) > 0) {
    wp %>%
      left_join(
        wr %>% select(dplyr::all_of("__fp_key"),
                      weekly_ecr_rank, weekly_ecr_best_rank, weekly_ecr_worst_rank,
                      weekly_ecr_avg_rank, weekly_ecr_sd_rank),
        by = "__fp_key"
      )
  } else {
    wp
  }
}

pull_ros_for_pos <- function(pg) {
  message(sprintf("[ffpros] ROS: %s (year=%d scoring=%s)", toupper(pg), YEAR, SCORING))
  df <- tryCatch(
    fp_projections(page = paste0("ros-", pg), year = YEAR, scoring = SCORING),
    error = function(e) {
      message(sprintf("  projections ros-%s not available; trying rankings ...", pg))
      tryCatch(
        fp_rankings(page = paste0("ros-", pg), year = YEAR),
        error = function(e2) { warning(sprintf("ROS pull failed for %s: %s", pg, e2$message)); tibble() }
      )
    }
  )
  norm_ros(df, pg)
}

# ---------- run & ranks ----------
weekly_list <- lapply(PAGES, pull_weekly_for_pos)
weekly <- suppressWarnings(bind_rows(weekly_list))

ros_list <- lapply(PAGES, pull_ros_for_pos)
ros <- suppressWarnings(bind_rows(ros_list))

# Points-based weekly ranks
if (nrow(weekly) > 0) {
  weekly <- weekly %>%
    group_by(year, week) %>%
    arrange(desc(weekly_avg), .by_group = TRUE) %>%
    mutate(weekly_points_overall_rank = dplyr::row_number()) %>%
    ungroup() %>%
    group_by(year, week, pos) %>%
    arrange(desc(weekly_avg), .by_group = TRUE) %>%
    mutate(weekly_points_pos_rank = dplyr::row_number()) %>%
    ungroup()
}

# ROS position rank
if (nrow(ros) > 0 && "ros_rank" %in% names(ros)) {
  ros <- ros %>%
    group_by(pos) %>%
    arrange(ros_rank, .by_group = TRUE) %>%
    mutate(ros_pos_rank = dplyr::row_number()) %>%
    ungroup()
}

message(sprintf("[ffpros] Weekly rows: %d | ROS rows: %d", nrow(weekly), nrow(ros)))

# ---------- write ----------
arrow::write_parquet(weekly, file.path(STAGING_DIR, "fp_weekly.parquet"))
arrow::write_parquet(ros,    file.path(STAGING_DIR, "fp_ros.parquet"))
safe_write_csv(weekly, file.path(STAGING_DIR, "fp_weekly.csv"))
safe_write_csv(ros,    file.path(STAGING_DIR, "fp_ros.csv"))
message("[ffpros] Wrote data/staging/fp_weekly.parquet and data/staging/fp_ros.parquet")
