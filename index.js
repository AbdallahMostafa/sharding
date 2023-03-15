const app = require('express')();
const { Client } = require('pg');
const HashRing = require('hashring');
const crypto = require('crypto');

const hr = new HashRing();
hr.add("5432");
hr.add("5433");
hr.add("5434");

const clinets = {
    "5432" : new Client({
        "host": "192.168.1.22" ,
        "port" : "5432",
        "user": "postgres",
        "password" : "password",
        "database" : "postgres"
    }),
    "5433" : new Client({
        "host": "192.168.1.22" ,
        "port" : "5433",
        "user": "postgres",
        "password" : "password",
        "database" : "postgres"
    }),
    "5434" : new Client ({
        "host": "192.168.1.22" ,
        "port" : "5434",
        "user": "postgres",
        "password" : "password",
        "database" : "postgres"
    })
}
connect();
async function connect() {
    await clinets['5432'].connect();
    await clinets['5433'].connect();
    await clinets['5434'].connect();
}
app.get("/:urlId", async(req, res) => {
    const urlId = req.params.urlId;
    const server = hr.get(urlId);
    const result = await  clinets[server].query("SELECT * FROM URL_TABLE WHERE URL_ID = $1", [urlId]);
    
    if(result.rowCount > 0) {
        res.send({
            "urlId": urlId,
            "url" : result.rows[0],
            "server": server
        })
    }else {
        res.sendStatus(404);
    }

})

app.post("/", async(req, res) => {
    const url = req.query.url;

    // consistently hash this to get a port
    const hash = crypto.createHash("sha256").update(url).digest('base64');
    const urlId = hash.substr(0, 5);

    const server = hr.get(urlId);
    
    await  clinets[server].query("INSERT INTO URL_TABLE (URL, URL_ID) VALUES ($1,$2)", [url, urlId]);
    
    res.send({
        "urlId": urlId,
        "url" : url,
        "server": server
    })
})

app.listen(8081, () => console.log("Listening to 8081"))