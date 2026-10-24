from pathlib import Path
import argparse
import json
import socket
import subprocess
import sys
import time
import urllib.request
import webbrowser

ROOT = Path(__file__).resolve().parents[2]
PYTHON = sys.executable
URL = 'http://127.0.0.1:8050'


def port_open(port):
    with socket.socket() as connection:
        connection.settimeout(0.2)
        return connection.connect_ex(('127.0.0.1', port)) == 0


def wait_for(check, process, name):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f'{name} stopped. Check assignment2/data/demo logs.')
        if check():
            return
        time.sleep(0.2)
    raise RuntimeError(f'{name} was not ready within 45 seconds.')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--auto', action='store_true')
    parser.add_argument('--no-browser', action='store_true')
    parser.add_argument('--interval', type=float, default=0.1)
    args = parser.parse_args()
    if args.interval < 0.1:
        parser.error('--interval must be at least 0.1')
    for port in (1883, 8050):
        if port_open(port):
            raise RuntimeError(f'Port {port} is in use. Stop the previous demo before restarting.')
    subprocess.run([PYTHON, 'assignment2/dashboard/prepare_demo_db.py', '--force'], cwd=ROOT, check=True)
    logdir = ROOT / 'assignment2/data/demo'
    received_path = logdir / 'received_events.jsonl'
    received_path.write_text('')
    expected_events = [json.loads(line) for line in (ROOT / 'assignment2/data/events/replay_events.jsonl').read_text().splitlines() if line.strip()]
    processes = []
    logs = []
    try:
        def start(script, logname, *extra):
            stream = (logdir / logname).open('w')
            logs.append(stream)
            child = subprocess.Popen([PYTHON, '-u', script, *extra], cwd=ROOT,
                                     stdout=stream, stderr=subprocess.STDOUT)
            processes.append(child)
            return child

        broker = start('assignment2/scripts/broker.py', 'broker.log')
        wait_for(lambda: 'MQTT broker ready' in (logdir / 'broker.log').read_text(), broker, 'MQTT broker')
        dashboard = start('assignment2/scripts/dashboard_entry.py', 'dashboard.log', '--database', 'assignment2/data/demo/demo_a2.duckdb', '--subscribe', '--mqtt-host', '127.0.0.1', '--received-log', str(received_path))

        def ready():
            if 'SUBSCRIBED' not in (logdir / 'dashboard.log').read_text():
                return False
            try:
                with urllib.request.urlopen(URL, timeout=1) as response:
                    return response.status == 200
            except OSError:
                return False

        wait_for(ready, dashboard, 'Dashboard')
        print(f'\nDashboard ready: {URL}\n', flush=True)
        if not args.no_browser:
            webbrowser.open(URL)
        if not args.auto:
            event_count = sum(bool(line.strip()) for line in (ROOT / 'assignment2/data/events/replay_events.jsonl').read_text().splitlines())
            input(f'Open the dashboard, then press Enter here to send {event_count} events: ')
        else:
            time.sleep(3)
        publisher = subprocess.Popen([
            PYTHON, '-u', 'assignment2/mqtt/publisher.py', '--host', '127.0.0.1',
            '--interval', str(args.interval), '--events', 'assignment2/data/events/replay_events.jsonl',
        ], cwd=ROOT)
        processes.append(publisher)
        result = publisher.wait()
        if result:
            raise RuntimeError('Publisher failed. Review the terminal output.')
        deadline = time.monotonic() + 120
        while True:
            received_events = [json.loads(line) for line in received_path.read_text().splitlines() if line.strip()]
            if received_events == expected_events:
                break
            if time.monotonic() >= deadline or dashboard.poll() is not None:
                raise RuntimeError(f'Replay verification failed: {len(received_events)}/{len(expected_events)} events received.')
            time.sleep(0.5)
        print(f'Verified: {len(received_events)} events received and stored.', flush=True)
        print('\nPublishing finished. Keep this terminal open to view the dashboard.\nPress Ctrl+C to stop. Run this command again for a fresh replay.', flush=True)
        while dashboard.poll() is None and broker.poll() is None:
            time.sleep(1)
        raise RuntimeError('A demo service stopped. Check the logs.')
    except (KeyboardInterrupt, EOFError):
        print('\nStopping demo...', flush=True)
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for stream in logs:
            stream.close()


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f'Error: {error}', file=sys.stderr)
        sys.exit(1)
