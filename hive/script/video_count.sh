#!/bin/bash
function func(){
	host=$1
	day_id=$2
	pattern=$3
	sql="use e_yalian;
		set mapreduce.map.memory.mb=4096; 
		set mapreduce.reduce.memory.mb=8192; 
		set mapreduce.map.java.opts=-Xmx3072m; 
		set mapreduce.reduce.java.opts=-Xmx6144m;
		select concat('http://$host',parse_url(url,'PATH')),count(*) c
		from dmp_ci_bh
		where comp_domain='$host'
			and day_id=$day_id
			and type='click'
			$pattern
		group by parse_url(url,'PATH')
		order by c desc
		limit 100;"
	echo $sql
	hive -e "$sql" > $day_id/${host}_$day_id
}
day_id=$1
mkdir $day_id
func tv.sohu.com $day_id "and parse_url(url,'PATH') like '/20%'"
func iqiyi.com $day_id "and parse_url(url,'PATH') rlike '^/._.*$'"
func v.qq.com $day_id "and parse_url(url,'PATH') like '/cover%'"
func letv.com $day_id "and parse_url(url,'PATH') like '/ptv%'"
func 56.com $day_id "and parse_url(url,'PATH') like '/u%'"
func v.youku.com $day_id "and parse_url(url,'PATH') like '/v_show%'"
func tudou.com $day_id "and parse_url(url,'PATH') like '/programs%' or parse_url(url,'PATH') like '/listplay%'"
func v.pptv.com $day_id "and parse_url(url,'PATH') like '/show%'"
func m1905.com $day_id "and parse_url(url,'PATH') like '/vod/play%'"
func fun.tv $day_id "and parse_url(url,'PATH') like '/vplay%'"
func v.ku6.com $day_id "and parse_url(url,'PATH') like '/show%'"
func baofeng.com $day_id "and parse_url(url,'PATH') like '/play%' or parse_url(url,'PATH') like '/micv%'"
func wasu.cn $day_id "and parse_url(url,'PATH') like '/Play%'"
func hunantv.com $day_id "and parse_url(url,'PATH') like '/v%'"
func vod.kankan.com $day_id "and parse_url(url,'PATH') like '/v%'"
func acfun.tv $day_id "and parse_url(url,'PATH') like '/v%'"
func bilibili.com $day_id "and parse_url(url,'PATH') like '/video%'"
func movie.douban.com $day_id "and parse_url(url,'PATH') like '/subject/%'"
