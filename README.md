# Monpoly Server

This project provides a python backend for MonPoly (https://bitbucket.org/jshs/monpoly). It requires an instance of a QuestDB database (https://questdb.io/get-questdb/).



## REST API endpoints

- `/` - displays info page
- `/get-policy` - returns the current policy
- `/set-policy` - sets the policy
- `/get-signature` - returns the current signature
- `/set-signature` - sets the signature
- `/log-events` - requires a JSON array of events to send to the monitor, it forwards them to the monitor and logs time points in QuestDB, if they are in order and otherwise correct

## how to use

1. Clone the latest version of the corresponding MonPoly branch (https://bitbucket.org/jshs/monpoly/src/BA_logging_backend/)
2. Follow the instructions in the README of MonPoly to set up the build environment and Dune
3. Use 
    ```
    cd monpoly
    dune build
    ```
    inside the repository to compile a binary of the latest version
4. Use the `Dockerfile.server` to build a docker image that can then be used with this backend. For this run:  
    ```
    docker build -t monpoly:dev -f Dockerfile.server .
    ```
    inside the repository.
5. Clone this repository (monpoly-server)
6. Use the Dockerfile in this repository to build another image  
    ```
    docker build -t monpoly-wrapper:dev .
    ```
7. Create a new container using this image:  
    ```
    docker run --net monpoly --name wrapper -v "[path to this repository on your system]:/monpoly-backend" -it -p 5000:5000 monpoly-wrapper:dev
    ```  
    or
    ```
    docker run --net monpoly --name wrapper -v "$(pwd):/monpoly-backend" -it -p 5000:5000 monpoly-wrapper:dev
    ```
    (using PowerShell and inside the repository)
8. To start the server, use: 
    ```
    flask --app=src/app.py run
    ```
    inside the container (either after the previous `docker run...` command or with `docker start wrapper` and `docker attach wrapper`)
