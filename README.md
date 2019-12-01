# big-fiubrother-scheduler
Big Fiubrother Scheduler Application

### Prerequisites

- python3

### Install

In order to install big-fiubrother-scheduler, a virtual environment is recommended. This can be achieved executing:

```
python3 -m venv big-fiubrother-scheduler-venv
source big-fiubrother-scheduler-venv/bin/activate
```

To install all the dependencies, execute the following command: 

```
python3 -m pip install -r requirements.txt
```

### Configuration

Before running, proper configuration should be considered. Inside the folder *config/* create a yaml file with the desired settings. By default, the application will try to load *config/development.yml*.

### Run

```
./run.py 
```
