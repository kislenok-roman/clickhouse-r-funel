CONST_BASE_VORONKI <- "
   select ring
   from {$localSwitch}.access_log
   where ring not in ('', 'NULL', 'N', 'deleted')
     and length(ring) >= 32
     and date = '{$dt}'
     {$sample}
     {$split}
   group by ring
   having count() between 2 and 10000 and sum(is_bot) = 0
"


