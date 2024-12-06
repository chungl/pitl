from consumer import SQLiteStore
import sched, time

if __name__ == "__main__":
    stores = [
        SQLiteStore(
            "/Users/casey/data/cats/",
            "weights.db",
            "measurements",
            "http://cats.local:8000",
        ),
        SQLiteStore(
            "/Users/casey/data/cats/",
            "weights2.db",
            "measurements",
            "http://cam.local:8000",
        ),
    ]
    period_s = 15

    def sync(store_index, scheduler):
        # schedule next
        scheduler.enter(
            period_s,
            1,
            sync,
            (
                store_index,
                scheduler,
            ),
        )
        # execute
        store = stores[store_index]
        try:
            store.catchup()
        except Exception as e:
            print(f"ERROR: catchup encountered exception {e}")

    def sync_clips(store_index, path, fps, scheduler):
        store = stores[store_index]
        try:
            store.catchup_files(f"{store.host}/clips", path, recursion_max=5, fps=fps)
        except Exception as e:
            print(f"ERROR: catchup_files encountered exception {e}")

        scheduler.enter(
            period_s,
            1,
            sync_clips,
            (
                store_index,
                path,
                fps,
                scheduler,
            ),
        )

    s = sched.scheduler(time.time, time.sleep)
    sync(0, s)
    sync(1, s)
    sync_clips(0, "/Users/casey/data/cats/clips", 3, s)
    sync_clips(1, "/Users/casey/data/cats/clips2", 30, s)
    s.run()
