from update_datasets import update_ds
from update_txs import update_txs

while True:
    try:
        update_ds()
    except Exception as e:
        continue

    sleep(10800)

    try:
        update_txs()
    except Exception as e:
        continue
    
    sleep(10800)
