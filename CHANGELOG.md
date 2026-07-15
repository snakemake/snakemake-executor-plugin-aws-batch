# Changelog

## [0.3.0](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/compare/v0.2.1...v0.3.0) (2026-07-15)


### Features

* add rule-specific container configuration option ([#30](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/issues/30)) ([#36](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/issues/36)) ([05d87f6](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/05d87f6a8319c04ff299985841a6946b5ca648e9))
* job scheduling priority via setting and per-rule resource ([#43](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/issues/43)) ([1f7703b](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/1f7703b661c299e7b86ae617e97cb1e919ad13a8))
* per-rule queues, per-job tags, EC2/Fargate-aware validation ([#40](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/issues/40)) ([1d7d6e6](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/1d7d6e6f60c0329c748e07a8aa8cd0da254b8868))
* per-rule task timeout via aws_batch_task_timeout resource ([#42](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/issues/42)) ([f708eac](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/f708eac39c26c9295d521c8be3c0b52bf8e5fbdd))
* support pre-existing job definitions via containerOverrides ([#44](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/issues/44)) ([3b26b40](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/3b26b4031c711a17ea6d97ee5f5e9cf7b5f94560))


### Bug Fixes

* Convert `log_info` dict to string for logging ([#35](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/issues/35)) ([1933da1](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/1933da1639fa60f9ccd3882280b352ef6211e590))
* don't apply a 300s default task timeout to every job ([#41](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/issues/41)) ([96f8161](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/96f816122dc6e424e80564cb19169e84794d8694))


### Documentation

* doc link in README.md ([#27](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/issues/27)) ([2525ca0](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/2525ca0ce5266fecc1fecd9f1750b2f8f397e560))

## [0.2.1](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/compare/v0.2.0...v0.2.1) (2025-03-14)


### Miscellaneous Chores

* release 0.2.1 ([0e58f7a](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/0e58f7a78fcfcb696126000b93b1c2e1f3f5d615))

## [0.2.0](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/compare/v0.1.0...v0.2.0) (2025-03-14)


### Features

* cleanup job definitions ([#20](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/issues/20)) ([c1843ec](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/c1843ec5969d9c14ceae20db986466c8b5571452))


### Miscellaneous Chores

* release 0.2.0 ([af1c0bc](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/af1c0bc619cfa4da7983b6e03a2a36405d28b7dd))
* release 0.2.0 ([d18ea37](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/d18ea3743340a3cb6a940d9b5bbb0e4dc3274b58))

## 0.1.0 (2025-03-03)


### Features

* executor settings ([bdc9f42](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/bdc9f42829077cf5edb8f5c2d928dd2ca6b83ede))
* post init, cancel ([42ce878](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/42ce878086dd1989dfa62362b948e22849c9ddf2))
* test structure ([5e86f91](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/5e86f91edb9e281832d2e65e8fcb39582fd9e3d8))


### Bug Fixes

* job_deploy_sources ([7934779](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/7934779732f4eab17790f28fffb5c6604e73b230))


### Documentation

* author ([f6cbd2f](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/f6cbd2f97c675305d51978774bd900ffbaa98cfc))
* update readme ([6191267](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/6191267004748f2247720e4ec14fda43a074c6c6))
* update readme ([15f72fb](https://github.com/snakemake/snakemake-executor-plugin-aws-batch/commit/15f72fb0fb2a8ad9bb6592b7e36ef78209f1c1b6))
