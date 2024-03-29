# OpenCalaisClient
This is an implementation of [Open Calais API](http://www.opencalais.com/) to extract semantic entities from input text or file. The Open Calais API provites "Intelligent Tagging" service as RESTful API. In order to use this API, you should acquire an access key. 


The repository includes:
* Source code
* Sample json file - contains youtube content context. This file is generated by [Youtube-dl](https://github.com/jungbomp/YouTubeWapper)
* Sample csv file - contains Person and Organization entities. This file is a result of Open Calais API with description texts of youtube-data.json

### Datasets

* Youtube-data.json - Key is url of the video.

### Compile & Run

```bash
$ cd OpenCalaisClient
$ mvn package
$ java -jar target/HttpClientCalaisPost-1.0-SNAPSHOT.jar
```

### Clean

```bash
$ cd OpenCalaisClient
$ mvn clean
```

### Status

Version 1.0
