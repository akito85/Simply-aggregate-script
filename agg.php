<?php

/* check the previous process if exists then dont start next process */

$check = shell_exec("ps ax | grep /opt/bin/agg.php");
print $check;
if(substr_count(trim($check),"/usr/bin/php /opt/bin/agg.php") > 1){
    die("Other process is still running, exit now.");
}


$mongo = new Mongo();

$suffix = date('mY',time());

$col = 'active_data_'.$suffix;

$aggcol = 'agg_data_'.$suffix;

/*
TO DO : validate aggregation time check, 
logically only current month active_data collection gets aggregated into current aggregate collection ( same suffix )
*/


/* get the timestamp in a file */
if(file_exists('/root/last_ts2.txt')){
    $last_ts = file_get_contents('/root/last_ts2.txt');
    if($last_ts == 0 || $last_ts == ''){
        $last_ts = false;
    }
}else{
    $last_ts = false;
}

$collection = $mongo->selectDB("drs_db")->selectCollection($col);
$collection->ensureIndex(array('capture_date' => 1 ));

$aggcollection = $mongo->selectDB("drs_db")->selectCollection($aggcol);
$aggcollection->ensureIndex(array('capture_date' => 1 ));

/* if there's no timestamp then find active_collection */
$cursor = $collection->find()->sort(array('capture_date'=>1))->limit(1);

/* get the bottom timestamp */
$t0 = iterator_to_array($cursor);

// for testing purposes
//print_r($t0);
//print $col;
//exit;


/* go to last record of active data */
$cursor->rewind();

$cursor = $collection->find()->sort(array('capture_date'=>-1))->limit(1);

$t1 = iterator_to_array($cursor); /* very last capture date */

$cursor->rewind();

$t0 = array_pop($t0);
$t0 = (int)$t0['capture_date'];

if($last_ts){
    $t0 = $last_ts;
}

print 't0 : '.$t0;

print "\r\n";

$t1 = array_pop($t1);
$t1 = (int)$t1['capture_date'];
print 't1 : '.$t1;

print "\r\n";
print 'delta t : '.(int)($t1-$t0);

print "\r\n";

$steps = (int)(($t1-$t0)/300);
print 'steps : '.$steps;

print "\r\n";

$tstart = $t0;

for($i = 0;$i < $steps;$i++){
    print 'step # : '.$i;
    print "\r\n";
    print 'tstart : '.$tstart;
    $tend = $tstart+300;
    print "\r\n";
    print 'tend : '.$tend ."\n". '$1 : ' . $t1;
    print "\r\n";
    print "======================================";
    print "\r\n";

    $cursor = $collection->find(array('capture_date'=>array('$gte'=>$tstart,'$lt'=>$tend)))->sort(array('capture_date'=>1));
    
    $results = iterator_to_array($cursor);
    
    print_r(iterator_to_array($cursor));
 
    // data summary
    $data = array();
    // app transport packet count
    $app = array();

    $data['tot_elapsed'] = 0;
    $data['tot_thruput_tcp'] = 0;
    $data['tot_thruput_udp'] = 0;
    $data['tot_truhput'] = 0;
    $data['tot_pkt'] = 0;
    $data['tot_pkt_tcp'] = 0;
    $data['tot_pkt_udp'] = 0;
    $data['tot_pkt_in'] = 0;
    $data['tot_pkt_out'] = 0;
    $data['tot_byte_in'] = 0;
    $data['tot_byte_out'] = 0;
    $data['tot_rxmt_pkt'] = 0;
    $data['tot_rxmt_byte'] = 0;
    $data['tot_avg_rtt'] = 0;
    $data['tot_tcp_con'] = 0;
    $data['tot_udp_con'] = 0;
    $data['tot_http'] = 0;
    $data['tot_proxy'] = 0;
    $data['tot_ntp'] = 0;
    $data['tot_ssh'] = 0;
    $data['tot_dns'] = 0;
    $data['tot_ssl'] = 0;
    $data['tot_pop3'] = 0;
    $data['tot_pop3s'] = 0;
    $data['tot_ym'] = 0;
    $data['tot_imap'] = 0;
    $data['tot_telnet'] = 0;
    $data['tot_smtp'] = 0;
    $data['tot_smtps'] = 0;
    $data['tot_smtpsub'] = 0;
    $data['tot_ftp'] = 0;
    $data['tot_snmp'] = 0;
    
    foreach($results as $r){
        //summarize packet data within time range
        $data["tot_elapsed"] += $r['elapsed_time'];
        $data["tot_thruput_udp"] += ($r['transport_protocol'] == 'UDP')?($r['src_truhput']+$r['dst_truhput']):0;
        $data["tot_truhput"] += ($r['src_truhput']+$r['dst_truhput']);
        $data["tot_pkt_tcp"] += ($r['transport_protocol'] == 'TCP')?($r['src_pkt_sent']+$r['dst_pkt_sent']):0;
        $data["tot_pkt_out"] += $r['src_pkt_sent'];
        $data["tot_pkt_udp"] += ($r['transport_protocol'] == 'UDP')?($r['src_pkt_sent']+$r['dst_pkt_sent']):0;
        $data["tot_rxmt_pkt"] += ($r['src_actl_rxmt_pkt']+$r['dst_actl_rxmt_pkt']);
        $data["tot_pkt_in"] += $r['dst_pkt_sent'];
        $data["tot_pkt"] += $r['tot_pkt'];
        $data["tot_byte_in"] += $r['dst_actl_byte_sent'];
        $data["tot_thruput_tcp"] += ($r['transport_protocol'] == 'TCP')?($r['src_truhput']+$r['dst_truhput']):0;
        $data["tot_rxmt_byte"] += $r['src_actl_byte_sent'] ;
        $data["tot_byte_out"] += $r['src_actl_byte_sent'] ;
        $data["tot_avg_rtt"] += $r['src_avg_rtt'];
        $data["tot_tcp_con"] += ($r['transport_protocol'] == 'TCP')?1:0;
        $data["tot_udp_con"] += ($r['transport_protocol'] == 'UDP')?1:0;
        
        //summarize app packet count within time range
        $data["tot_http"] += ($r['apps_protocol'] == 'HTTP')?1:0;
        $data["tot_proxy"] += ($r['apps_protocol'] == 'HTTP/PROXY')?1:0;
        $data["tot_ntp"] += ($r['apps_protocol'] == 'NTP')?1:0;
        $data["tot_snmp"] += ($r['apps_protocol'] == 'SNMP')?1:0;
        $data["tot_ssh"] += ($r['apps_protocol'] == 'SSH')?1:0;
        $data["tot_dns"] += ($r['apps_protocol'] == 'DNS')?1:0;
        $data["tot_ssl"] += ($r['apps_protocol'] == 'SSL/TLS')?1:0;
        $data["tot_pop3"] += ($r['apps_protocol'] == 'POP3')?1:0;
        $data["tot_pop3s"] += ($r['apps_protocol'] == 'POP3 SSL/TLS')?1:0;
        $data["tot_ym"] += ($r['apps_protocol'] == 'YM')?1:0;
        $data["tot_imap"] += ($r['apps_protocol'] == 'IMAP SSL/TLS')?1:0;
        $data["tot_telnet"] += ($r['apps_protocol'] == 'TELNET')?1:0;
        $data["tot_smtp"] += ($r['apps_protocol'] == 'SMTP')?1:0;
        $data["tot_smtps"] += ($r['apps_protocol'] == 'SMTP SSL/TLS')?1:0;
        $data["tot_smtpsub"] += ($r['apps_protocol'] == 'SMTP Submissions')?1:0;
        $data["tot_ftp"] += ($r['apps_protocol'] == 'FTP')?1:0;

        //remove collection
        $collection->remove(array( '_id' => $r['_id']),true);
        
        //summarize unique ip within time range
        /*
        if(!in_array($r['dst_ipaddr'],$dst_ip_array)){
            $dst_ip_array[] = $r['dst_ipaddr'];
            $dst_ip_count++;
        }

        if(!in_array($r['src_ipaddr'],$src_ip_array)){
            $src_ip_array[] = $r['src_ipaddr'];
            $src_ip_count++;
        }
        */
    }

    $data["timestamp"] = $tend;

    $check_ts = $aggcollection->find(array('timestamp'=>$tend));
    $check_ts = iterator_to_array($check_ts);

    if(count($check_ts) < 1){
        $aggcollection->insert($data);
        print "duplicate data found, skip insert\r\n";
    }
    
    print_r($data);

    $cursor->rewind();
    
    $tstart = $tend;

}

file_put_contents('/root/last_ts2.txt',$tend);

?>
