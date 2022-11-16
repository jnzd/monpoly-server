# Monpoly Server

This project provides a python backend for Monpoly (https://bitbucket.org/jshs/monpol). It requires an instance of a QuestDB database (https://questdb.io/get-questdb/).



## REST API endpoints

- `/` - displays info page
- `/get-policy` - returns the current policy
- `/set-policy` - sets the policy
- `/get-signature` - returns the current signature
- `/set-signature` - sets the signature
- `/log-events` - requires a json array of events to send to the monitor, it forwards them to the monitor and logs timepoints in questdb, if they are in order and otherwise correct

## how to use

1. Clone the latest version of the corresponding Monpoly branch (https://bitbucket.org/jshs/monpoly/src/BA_logging_backend/)
2. Follow the instructions in the README of Monpoly to set up the build environment and Dune
3. Use `dune build` inside the repository to compile a binary of the latest version
4. Use the `Dockerfile.server` to build a docker image that can then be used with this backend. For this run:  
`docker build -t monpoly-server:dev -f Dockerfile.server .` inside the repository.
5. Clone this repository here (monpoly-server)
6. Use the Dockerfile in this repository to build another image  
`docker build -t monpoly-backend:dev .`
7. Create a new container using this image:  
`docker run --net monpoly --name backend -v "[path to this repository on your system]:/monpoly-backend" -it -p 5000:5000 monpoly-backend:dev`  
or `docker run --net monpoly --name backend -v "$(pwd):/monpoly-backend" -it -p 5000:5000 monpoly-backend:dev`  (using powershell and inside the repository)
8. To start the server, use: `flask --app=src/app.py` inside the container (either after the previous `docker run...` command or with `docker start backend` and `docker attach backend`)
