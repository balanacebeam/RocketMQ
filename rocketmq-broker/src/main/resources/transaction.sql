CREATE TABLE `t_transaction` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `broker_name` varchar(50) NOT NULL,
  `offset` bigint(20) unsigned NOT NULL,
  `producer_group` varchar(50) NOT NULL,
  `gmt_create` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `bpo_idx` (`broker_name`,`producer_group`,`offset`),
  KEY `bpg_idx` (`broker_name`,`producer_group`,`gmt_create`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8