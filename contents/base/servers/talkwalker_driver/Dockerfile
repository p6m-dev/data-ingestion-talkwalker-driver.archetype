#Note: build this docker image from legos root directory
FROM ubuntu:latest
RUN apt update && apt upgrade -y
RUN apt install -y python3-pip

RUN mkdir /app
#WORKDIR /app
#RUN ls -alh ..

# Copy the current directory contents into the container at /app
#COPY ../libraries/  app/libraries/
#COPY   .  app/
#RUN ls ../libraries

COPY libraries/ app/libraries
COPY talkwalker_driver/ app/

RUN pip3 install -r app/talkwalker_requirements.txt
#ENV PYTHONUNBUFFERED=1
##ENTRYPOINT ["/bin/sleep", "1d"]
#CMD /bin/sleep 1d

ENTRYPOINT ["python3", "/app/talkwalker_main.py"]