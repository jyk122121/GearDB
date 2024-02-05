echo "GearDB"

#../out-static/db_bench --benchmarks=fillrandom,stats --value_size=800 --compression_ratio=0.5 --num=300000000 --threads=1
#../out-static/db_bench --benchmarks=overwrite,stats --use_existing_db=1 --value_size=800 --compression_ratio=0.5 --num=300000000 --threads=1

../out-static/db_bench --benchmarks=fillrandom,overwrite,stats --value_size=800 --compression_ratio=0.5 --num=3000000 --threads=1
#../out-static/db_bench --benchmarks=overwrite,stats --use_existing_db=1 --value_size=800 --compression_ratio=0.5 --num=1000000 --threads=1

#../out-static/db_bench --benchmarks=fillrandom --value_size=800 --compression_ratio=0.5 --num=10000000 --threads=1
#../out-static/db_bench --benchmarks=fillrandom --value_size=1024 --num=1000000000 --threads=1
