import subprocess, time


print('Creating database...')
start  = time.perf_counter()
subprocess.run(['python', 'create_database.py'])
end  = time.perf_counter()
print(f'Database created ({round((end-start)*1000)} ms).')
print()

print('Running ETL process...')
start  = time.perf_counter()
subprocess.run(['python', 'etl.py'])
end  = time.perf_counter()
print(f'ETL process complete ({round((end-start))} sec).')
print()

print("Done.")