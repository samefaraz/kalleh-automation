#!/bin/sh
# Exit immediately if a command exits with a non-zero status.
set -e

echo "--- Setting up virtual environment ---"
python -m venv /app/venv

echo "--- Installing requirements ---"
if [ -s "/app/requirements.txt" ]; then
    # Use --quiet to make pip less verbose
    /app/venv/bin/pip install --no-cache-dir --quiet -r /app/requirements.txt
else
    echo "No requirements to install."
fi

echo "--- Running user code ---"
# Run the user's script. The output of this command will be the container's logs.
/app/venv/bin/python /app/code.py