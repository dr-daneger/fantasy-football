ROS Data Sources and Verification

- Source library: R `ffpros` (`fp_rankings`) with `page = 'ros-<pos>'` and `year = <YEAR>`.
- FantasyPros pages (manually verifiable):
  - Overall: https://www.fantasypros.com/nfl/rankings/ros-overall.php
  - QB:      https://www.fantasypros.com/nfl/rankings/ros-qb.php
  - RB:      https://www.fantasypros.com/nfl/rankings/ros-rb.php
  - WR:      https://www.fantasypros.com/nfl/rankings/ros-wr.php
  - TE:      https://www.fantasypros.com/nfl/rankings/ros-te.php
  - K:       https://www.fantasypros.com/nfl/rankings/ros-k.php
  - DST:     https://www.fantasypros.com/nfl/rankings/ros-dst.php

How it maps into JSON
- The `projections.ros` object is built from `data/staging/fp_ros.parquet` (written by `r/ffpros_ingest.R`).
- Fields:
  - `rank`: FantasyPros ROS ECR rank.
  - `pos_rank`: Position rank computed per‑position in our pipeline.
  - `best_rank`, `worst_rank`, `avg_rank`, `sd_rank`: dispersion where available.
  - `pulled_at`: timestamp from the R ingestor.

Verification quick steps
1) Run: `FP_YEAR=2025 Rscript r/ffpros_ingest.R`
2) Inspect: `data/staging/fp_ros.parquet` (or `fp_ros.csv`) for a player/position.
3) Cross‑check the player’s row with the corresponding ROS page above.

Note on weekly JSON
- Weekly JSON no longer includes ECR (`ecr_*`) or uncertainty (`sd`, `floor`, `ceiling`) fields.
- Weekly ranks are points‑based from projections: `rank_overall`, `rank_pos`.
