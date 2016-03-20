CREATE TABLE `t_transaction` (
  `id`             BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `broker_name`    VARCHAR(50)         NOT NULL,
  `offset`         BIGINT(20) UNSIGNED NOT NULL,
  `producer_group` VARCHAR(50)         NOT NULL,
  `gmt_create`     DATETIME            NOT NULL,
  PRIMARY KEY (`id`),
  KEY `bpo_idx` (`broker_name`, `producer_group`, `offset`),
  KEY `bpg_idx` (`broker_name`, `producer_group`, `gmt_create`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8