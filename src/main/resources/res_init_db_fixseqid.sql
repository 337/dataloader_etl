

create table if not exists `sys_meta` ( `prop_name` varchar(255) binary not null, `prop_type` enum('sql_string','sql_bigint','sql_datetime') not null,`prop_func` enum('once','cover','inc') not null,`prop_orig` enum('sys','user') not null,`prop_alias` varchar(255),`prop_segm` text,primary key(`prop_name`) )ENGINE=InnoDB DEFAULT CHARSET=utf8;


create table if not exists `register_time`  ( uid bigint primary key, val bigint not null, key `idx_val`(`val`) )engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `last_login_time`( uid bigint primary key, val bigint not null, key `idx_val`(`val`) )engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `first_pay_time` ( uid bigint primary key, val bigint not null, key `idx_val`(`val`) )engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `last_pay_time`  ( uid bigint primary key, val bigint not null, key `idx_val`(`val`) )engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `grade`          ( uid bigint primary key, val bigint not null, key `idx_val`(`val`) )engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `pay_amount`     ( uid bigint primary key, val bigint not null, key `idx_val`(`val`) )engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `language`       ( uid bigint primary key, val varchar(255) not null, key `idx_val` (`val`) )engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `version`        ( uid bigint primary key, val varchar(255) not null, key `idx_val` (`val`) )engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `platform`       ( uid bigint primary key, val varchar(255) not null, key `idx_val` (`val`) )engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `identifier`     ( uid bigint primary key, val varchar(255) not null, key `idx_val` (`val`) )engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `ref`           ( uid bigint primary key, val varchar(255) not null, key `idx_val` (`val`) )engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `ref0`           ( uid bigint primary key, val varchar(255) not null, key `idx_val` (`val`) )engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `ref1`           ( uid bigint primary key, val varchar(255) not null, key `idx_val` (`val`))engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `ref2`           ( uid bigint primary key, val varchar(255) not null, key `idx_val` (`val`))engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `ref3`           ( uid bigint primary key, val varchar(255) not null, key `idx_val` (`val`))engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `ref4`           ( uid bigint primary key, val varchar(255) not null, key `idx_val` (`val`))engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `nation`           ( uid bigint primary key, val varchar(255) not null, key `idx_val` (`val`))engine=InnoDB DEFAULT CHARSET=utf8;
create table if not exists `geoip`           ( uid bigint primary key, val varchar(255) not null, key `idx_val` (`val`))engine=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE if not exists `coin_buy`           (`uid` bigint(20) NOT NULL, `val` bigint(20) NOT NULL,PRIMARY KEY (`uid`),KEY `idx_val` (`val`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE if not exists `coin_initialstatus` (`uid` bigint(20) NOT NULL, `val` bigint(20) NOT NULL,PRIMARY KEY (`uid`),KEY `idx_val` (`val`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE if not exists `coin_promotion`     (`uid` bigint(20) NOT NULL, `val` bigint(20) NOT NULL,PRIMARY KEY (`uid`),KEY `idx_val` (`val`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;


create table if not exists `cold_user_info` ( uid bigint primary key, val text not null )engine=InnoDB DEFAULT CHARSET=utf8;


replace into `sys_meta` set `prop_name`='register_time'      ,prop_type='sql_datetime'       ,prop_func='once'    ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='last_login_time'    ,prop_type='sql_datetime'       ,prop_func='cover'   ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='first_pay_time'     ,prop_type='sql_datetime'       ,prop_func='once'    ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='last_pay_time'      ,prop_type='sql_datetime'       ,prop_func='cover'   ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='grade'              ,prop_type='sql_bigint'       ,prop_func='cover'   ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='pay_amount'         ,prop_type='sql_bigint'       ,prop_func='inc'     ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='language'           ,prop_type='sql_string'       ,prop_func='cover'   ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='version'            ,prop_type='sql_string'       ,prop_func='cover'   ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='platform'           ,prop_type='sql_string'       ,prop_func='cover'   ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='identifier'         ,prop_type='sql_string'       ,prop_func='cover'   ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='ref'                ,prop_type='sql_string'       ,prop_func='once'    ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='ref0'                ,prop_type='sql_string'       ,prop_func='once'    ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='ref1'                ,prop_type='sql_string'       ,prop_func='once' ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='ref2'                ,prop_type='sql_string'       ,prop_func='once' ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='ref3'                ,prop_type='sql_string'       ,prop_func='once' ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='ref4'                ,prop_type='sql_string'       ,prop_func='once' ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='nation'                ,prop_type='sql_string'       ,prop_func='once' ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='geoip'                ,prop_type='sql_string'       ,prop_func='once' ,prop_orig='sys'    ;

replace into `sys_meta` set `prop_name`='coin_buy'         ,prop_type='sql_bigint'       ,prop_func='inc'    ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='coin_initialstatus'         ,prop_type='sql_bigint'       ,prop_func='inc'    ,prop_orig='sys'    ;
replace into `sys_meta` set `prop_name`='coin_promotion'         ,prop_type='sql_bigint'       ,prop_func='inc'    ,prop_orig='sys'    ;
