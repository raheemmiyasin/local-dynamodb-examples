const AWS = require('aws-sdk');
const moviedata = require('./moviedata.json');

AWS.config.update({
    region: "us-west-2",
    endpoint: 'http://localhost:8000',
    // accessKeyId default can be used while using the downloadable version of DynamoDB. 
    // For security reasons, do not store AWS Credentials in your files. Use Amazon Cognito instead.
    accessKeyId: "fakeMyKeyId",
    // secretAccessKey default can be used while using the downloadable version of DynamoDB. 
    // For security reasons, do not store AWS Credentials in your files. Use Amazon Cognito instead.
    secretAccessKey: "fakeSecretAccessKey"
});

const dynamodb = new AWS.DynamoDB();

function createMovies() {
    console.log('create movies table');
    var params = {
        TableName: "Movies",
        KeySchema: [
            { AttributeName: "year", KeyType: "HASH" },
            { AttributeName: "title", KeyType: "RANGE" }
        ],
        AttributeDefinitions: [
            { AttributeName: "year", AttributeType: "N" },
            { AttributeName: "title", AttributeType: "S" }
        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: 5,
            WriteCapacityUnits: 5
        }
    };

    dynamodb.createTable(params, function (err, data) {
        if (err) {
            console.log(err);
        } else {
            console.log(data);
        }
    });
}

const docClient = new AWS.DynamoDB.DocumentClient();

function processFile() {

    var allMovies = moviedata;

    allMovies.forEach(function (movie) {
        console.log(movie);
        var params = {
            TableName: "Movies",
            Item: {
                "year": movie.year,
                "title": movie.title,
                "info": movie.info
            }
        };
        docClient.put(params, function (err, data) {
            if (err) {
                console.log(err);
            } else {
                console.log('PutItem succeeded: ' + movie.title);
            }
        });
    });
}

function createItem() {
    var params = {
        TableName :"Movies",
        Item:{
            "year": 2015,
            "title": "The Big New Movie",
            "info":{
                "plot": "Nothing happens at all.",
                "rating": 0
            }
        }
    };
    docClient.put(params, function(err, data) {
        if (err) {
            console.log(err);
        } else {
            console.log(data);
        }
    });
}

function readItem() {
    var table = "Movies";
    var year = 2015;
    var title = "The Big New Movie";

    var params = {
        TableName: table,
        Key:{
            "year": year,
            "title": title
        }
    };
    docClient.get(params, function(err, data) {
        if (err) {
            console.log(err);
        } else {
            console.log(data);
        }
    });
}

function updateItem() {
    var table = "Movies";
    var year = 2015;
    var title = "The Big New Movie";

    var params = {
        TableName:table,
        Key:{
            "year": year,
            "title": title
        },
        UpdateExpression: "set info.rating = :r, info.plot=:p, info.actors=:a",
        ExpressionAttributeValues:{
            ":r":5.5,
            ":p":"Everything happens all at once.",
            ":a":["Larry", "Moe", "Curly"]
        },
        ReturnValues:"UPDATED_NEW"
    };

    docClient.update(params, function(err, data) {
        if (err) {
            console.log(err);
        } else {
            console.log(data);
        }
    });
}

function increaseRating() {
    var table = "Movies";
    var year = 2015;
    var title = "The Big New Movie";

    var params = {
        TableName:table,
        Key:{
            "year": year,
            "title": title
        },
        UpdateExpression: "set info.rating = info.rating + :val",
        ExpressionAttributeValues:{
            ":val":1
        },
        ReturnValues:"UPDATED_NEW"
    };

    docClient.update(params, function(err, data) {
        if (err) {
            console.log(err);
        } else {
            console.log(data);
        }
    });
}

function conditionalUpdate() {
    var table = "Movies";
    var year = 2015;
    var title = "The Big New Movie";

    var params = {
        TableName:table,
        Key:{
            "year": year,
            "title": title
        },
        UpdateExpression: "remove info.actors[0]",
        ConditionExpression: "size(info.actors) >= :num",
        ExpressionAttributeValues:{
            ":num":3
        },
        ReturnValues:"UPDATED_NEW"
    };

    docClient.update(params, function(err, data) {
        if (err) {
            console.log(err);
        } else {
            console.log(data);
        }
    });
}

function conditionalDelete() {
    var table = "Movies";
    var year = 2015;
    var title = "The Big New Movie";

    var params = {
        TableName:table,
        Key:{
            "year":year,
            "title":title
        },
        ConditionExpression:"info.rating >= :val",
        ExpressionAttributeValues: {
            ":val": 5.0
        }
    };

    docClient.delete(params, function(err, data) {
        if (err) {
            console.log(err);
        } else {
            console.log(data);
        }
    });
}

function queryData() {
    console.log("Querying for movies from 1985.");

    var params = {
        TableName : "Movies",
        KeyConditionExpression: "#yr = :yyyy",
        ExpressionAttributeNames:{
            "#yr": "year"
        },
        ExpressionAttributeValues: {
            ":yyyy":1985
        }
    };

    docClient.query(params, function(err, data) {
        if (err) {
            console.log(err);
        } else {
            console.log(data);
        }
    });
}

function queryData2() {
    console.log("Querying for movies from 1992 - titles A-L, with genres and lead actor: ");

    var params = {
        TableName : "Movies",
        ProjectionExpression:"#yr, title, info.genres, info.actors[0]",
        KeyConditionExpression: "#yr = :yyyy and title between :letter1 and :letter2",
        ExpressionAttributeNames:{
            "#yr": "year"
        },
        ExpressionAttributeValues: {
            ":yyyy":1992,
            ":letter1": "A",
            ":letter2": "L"
        }
    };

    docClient.query(params, function(err, data) {
        if (err) {
            console.log(err);
        } else {  
             console.log(data);
        }
    });
}

function scanData() {
    console.log("Scanning Movies table.");

    var params = {
        TableName: "Movies",
        ProjectionExpression: "#yr, title, info.rating",
        FilterExpression: "#yr between :start_yr and :end_yr",
        ExpressionAttributeNames: {
            "#yr": "year",
        },
        ExpressionAttributeValues: {
            ":start_yr": 1950,
            ":end_yr": 1959
        }
    };

    docClient.scan(params, onScan);

    function onScan(err, data) {
        if (err) {
            console.log(err);
        } else {
            // Print all the movies
            console.log("Scan succeeded. ");
            data.Items.forEach(function(movie) {
                console.log(movie.year + ": " + movie.title + " - rating: " + movie.info.rating + "\n");
            });

            // Continue scanning if we have more movies (per scan 1MB limitation)
            // console.log("Scanning for more...");
            // params.ExclusiveStartKey = data.LastEvaluatedKey;
            // docClient.scan(params, onScan);            
        }
    }
}

function deleteMovies() {
    var params = {
        TableName : "Movies"
    };

    dynamodb.deleteTable(params, function(err, data) {
        if (err) {
            console.log(err);
        } else {  
             console.log(data);
        }
    });
}

//createMovies();
//processFile();
//createItem();
//readItem();
//updateItem();
//increaseRating();
//conditionalUpdate();
//conditionalDelete();
//queryData();
//queryData2();
//scanData();
//deleteMovies()
