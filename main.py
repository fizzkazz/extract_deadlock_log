import os
import json


def remove_extension(filename):
    root, _ = os.path.splitext(filename)
    return root


if __name__ == "__main__":
    # Read all json files in the import folder
    import_file_list = os.listdir('data/import/')
    import_file_list = [f for f in import_file_list if f.endswith(
        '.json') or f.endswith('.jsonl')]
    for ipath in import_file_list:
        data_concatenated = []
        with open(f'data/import/{ipath}', 'r') as f:
            data_concatenated.extend(json.load(f))

    # Remove duplicate logs
    seen_timestamp = set()
    unique_data_concatenated = []
    for d in data_concatenated:
        if d['timestamp'] not in seen_timestamp:
            seen_timestamp.add(d['timestamp'])
            unique_data_concatenated.append(d)

    # Extract deadlock logs
    deadlock_logs = []
    deadlock_log_draft = []
    deadlock_log_draft_timestamp = ''
    deadlock_detected = False
    for udc in unique_data_concatenated:
        line = udc['textPayload']
        if '[InnoDB] Transactions deadlock detected, dumping detailed information.' in line:
            deadlock_log_draft.append(line)
            deadlock_detected = True
            deadlock_log_draft_timestamp = udc['timestamp']
        elif deadlock_detected:
            deadlock_log_draft.append(line)
            # Check if the deadlock log ends
            if 'WE ROLL BACK TRANSACTION' in line:
                # Append the draft to the final list
                deadlock_logs.append(
                    {'timestamp': deadlock_log_draft_timestamp, "log": deadlock_log_draft})
                # Reset the draft
                deadlock_log_draft = []
                deadlock_log_draft_timestamp = ''
                deadlock_detected = False

    export_path = f'data/export/{remove_extension(ipath)}_extracted.json'
    with open(export_path, 'w') as f:
        json.dump(deadlock_logs, f)
    for log in deadlock_logs:
        with open(f'data/export/{remove_extension(ipath)}_extracted.txt', 'a') as f:
            f.write('-' * 50 + f'DEADLOCK AT ' +
                    log['timestamp'] + '-' * 50 + '\n')
            f.write('\n'.join(log['log']) + '\n')
