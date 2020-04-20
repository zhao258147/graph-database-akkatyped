drop keyspace graphakka;
drop keyspace graphakka_snapshot ;
drop keyspace userakka;
drop keyspace userakka_snapshot ;
truncate graph.nodes;
truncate graph.clicks;
truncate graph.read_side_offsets;