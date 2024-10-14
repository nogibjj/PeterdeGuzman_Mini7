
```sql
 SELECT voted_party_desc, COUNT(*) AS total_count FROM ped19_voterreg AS t1 JOIN ped19_voterhist AS t2 ON t1.ncid = t2.ncid GROUP BY voted_party_desc ORDER BY total_count DESC 
```

```response from databricks
[Row(voted_party_desc='DEMOCRATIC', total_count=88), Row(voted_party_desc='UNAFFILIATED', total_count=68), Row(voted_party_desc='REPUBLICAN', total_count=48)]
```

