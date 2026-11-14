from task_scheduler.storage import create_storage_app, initialize_schema


def main():
    app = create_storage_app()
    with app.app_context():
        initialize_schema()
    print("Scheduler-owned tables ready; existing Order tables preserved.", flush=True)


if __name__ == "__main__":
    main()
