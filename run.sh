echo $1
echo $2

if [ $1 == "DOCKER_BUILD" ]
then
    docker build -t nfrribeiro/zenith-integration .    
elif [ $1 == "DOCKER_PUSH" ]
then
    ./run.sh DOCKER_BUILD     
    docker push nfrribeiro/zenith-integration:latest
elif [ $1 == "DOCKER_RUN" ]
then
    #docker run -p 22:22 -d atmoz/sftp foo:pass:::upload
    ./run.sh DOCKER_BUILD  
    docker run --env-file .$2.env nfrribeiro/zenith-integration
elif [ $1 == "DOCKER_START" ]
then
    #docker run -p 22:22 -d atmoz/sftp foo:pass:::upload
    ./run.sh DOCKER_BUILD   
    docker run -d --env-file .$2.env nfrribeiro/zenith-integration
elif [ $1 == "BUILD" ]
then
    pip3.10 install -r requirements.txt
elif [ $1 == "RUN" ]
then
    export ZENITH_ENV=.$2.env
    python3.10 main.py
else
    echo "cenas"
fi