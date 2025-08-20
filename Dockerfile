FROM anasty17/mltb:latest

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y mkvtoolnix unrar p7zip-full ffmpeg && rm -rf /var/lib/apt/lists/*

RUN chmod 777 /usr/src/app

RUN python3 -m venv mltbenv

COPY requirements.txt .
RUN mltbenv/bin/pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["bash", "start.sh"]
