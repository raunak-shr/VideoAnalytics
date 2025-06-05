# VideoAnalytics - Gstreamer | RabbitMQ
---
Framework for producing frames from RTSP streams parallelly, using Gstreamer python bindings and storing in RabbitMQ Queues. 

## Step1: Required Default Installations

- Python Miniconda Distribution (`Miniconda3-py310_24.7.1-0-Windows-x86_64`)
- Erlang (`otp_win64_26.0`) 
- RabbitMQ Server (`rabbitmq-server-3.13.7`)  
- Gstreamer Runtime (`gstreamer-1.0-msvc-x86_64-1.24.7`) and Development (`gstreamer-1.0-devel-msvc-x86_64-1.24.7`).


## Step2: Add environment variables

Add the following paths to the environment - 

- `C:\Program Files\RabbitMQ Server\rabbitmq_server-3.13.7\sbin`
- `C:\ProgramData\miniconda3\condabin`
- `C:\gstreamer\1.0\msvc_x86_64\bin`
- `C:\gstreamer\1.0\msvc_x86_64\lib`
- `C:\gstreamer\1.0\msvc_x86_64\include`

Reboot the system at this step to let the changes take effect

## Step3: Verify Installations

Once completed, verify installations using the following commands in terminal - 

- `conda -V` for miniconda
- `rabbitmqctl version` for RabbitMQ
- `gst-launch-1.0 --gst-version` for Gstreamer


## Step4: Create producer environment 

Enable RabbitMQ Management plugin using 

`rabbitmq-plugins enable rabbitmq_management`

Once installed, restart the rabbitmq server for changes to take effect. Once restarted go to browser and visit `http://localhost:15672/#/`. For login use `guest` as both UserID and Password.

On local, open terminal and run

```
conda create --name gstenv python=3.10 conda-forge::pyturbojpeg conda-forge::gst-plugins-good=1.22.9 conda-forge::gst-plugins-bad=1.22.9 conda-forge::gst-plugins-ugly=1.22.9 conda-forge::gst-python=1.22.9 conda-forge::gst-libav=1.22.9 pandas openpyxl pika conda-forge::python-dotenv -y
```

## Step5: Launch

In project directory terminal run - 

- `conda activate gstenv` to activate environment. 
- `python producer.py` to run the frames producer.

View the frames production in the Management UI.
