
SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS `proxies`;
CREATE TABLE `proxies` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `proxy` varchar(25) DEFAULT NULL,
    `status` varchar(255) DEFAULT NULL,
    `ts` int(11) DEFAULT NULL,
    `counter` int(10) DEFAULT NULL,
    `web` varchar(255) DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `idx_proxy` (`proxy`, `web`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `ua`;
CREATE TABLE `ua` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `ua` varchar(500) DEFAULT NULL,
    `browser` varchar(255) DEFAULT NULL,
    `status` varchar(255) DEFAULT "1",
    `ts` int(11) DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `idx_ua` (`ua`) USING HASH
)ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `cookies`;
CREATE TABLE `cookies` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `cookies` text DEFAULT NULL,
    `proxy_id` int(11) DEFAULT NULL,
    `proxy` varchar(255) DEFAULT NULL,
    `ua_id` int(11) DEFAULT NULL,
    `ua` varchar(500) DEFAULT NULL,
    `counter` int(11) DEFAULT 0,
    `status` varchar(255) DEFAULT "0",
    `info` text DEFAULT NULL,
    `web` varchar(255) DEFAULT NULL,
    `ts` int(11) DEFAULT NULL,

    PRIMARY KEY (`id`)
)ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;