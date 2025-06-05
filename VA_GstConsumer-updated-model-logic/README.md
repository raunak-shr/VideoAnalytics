# VideoAnalytics - Gstreamer | RabbitMQ

---
Framework for consuming frames from RTSP streams parallely, using RabbitMQ Queues. 

## Step1: Required Default Installations

- Python Miniconda Distribution (`Miniconda3-py310_24.7.1-0-Windows-x86_64`)
- Erlang (`otp_win64_26.0`) 
- RabbitMQ Server (`rabbitmq-server-3.13.7`)
- Cuda Toolkit (`cuda_11.8.0_windows_network`) (for GPU Based Inference)
- MySQL Server (`mysql-installer-community-8.0.37.0`)

## Step2: Add environment variables

Add the following paths to the environment - 

- `C:\Program Files\RabbitMQ Server\rabbitmq_server-3.13.7\sbin`
- `C:\ProgramData\miniconda3\condabin`

Reboot the system at this step to let the changes take effect

## Step3: Verify Installations

Once completed, verify installations using the following commands in terminal - 

- `conda -V` for miniconda
- `rabbitmqctl version` for RabbitMQ

## Step4: Create consumer environment 

Enable RabbitMQ Management plugin using 
```
> rabbitmq-plugins enable rabbitmq_management
```

Once installed, restart the rabbitmq server for changes to take effect. 
Once restarted go to browser and if consuming on same system 
visit `http://localhost:15672/#/`. For login use `guest` as both UserID and Password.

If consuming from different system, visit `http://<producer-sys-ipv4-address>:15672/#/` and 
login with consumer system credentials (needs to be created on producer side
with admin privileges and access to the virtual host '\' via the management UI).

In the code's `consumer.py` file modify the RabbitMQ connection params accordingly.

For GPU Based Inference on local, open terminal and run

```
> conda create --name conenv python=3.10 conda-forge::mysql-connector-python pika conda-forge::pyturbojpeg conda-forge::dill openpyxl conda-forge::lap conda-forge::python-dotenv -y
> conda activate conenv
> conda install -c pytorch -c nvidia -c conda-forge pytorch torchvision pytorch-cuda=11.8 ultralytics
```

## Step5: Launch

In project directory terminal run - 

- `conda activate conenv` to activate environment. 
- `python consumer.py` to run the frames' consumer.

View the frames consumption in the Management UI.
