import psutil
import time

def monitor_cpu():
    while True:
        cpu_usage = psutil.cpu_percent(interval=1, percpu=True)
        print(f"CPU Usage per core: {cpu_usage}")
        total_cpu_usage = psutil.cpu_percent(interval=1)
        print(f"Total CPU Usage: {total_cpu_usage}%")
        time.sleep(2)  # Monitor every 2 seconds

if __name__ == '__main__':
    monitor_cpu()
