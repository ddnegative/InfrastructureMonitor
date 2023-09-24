const sql = require("mysql");
const axios = require("axios");
const crypto  = require("crypto");
const ping = require("ping");
const { EventEmitter } = require("events");

require("dotenv").config();

debug = false;
if (process.env?.DEBUG == "true") debug = true;
console.debug = debug ? console.log : () => null;

const mapResultLabelsToData = json => {
    if (Object.keys(json.data).length > 0) {
        let [data, parsed_data] = [json.data[json.data.length - 1], {}];
        json.labels.forEach((value, index) => value !== "time" && (parsed_data[value] = data[index]));
        return parsed_data
    }
}

class Server {
    constructor(address, hostname, type, timeout) {
        this.address = address;
        this.hostname = hostname;
        this.type = type;
        this.timeout = timeout ?? 300;
    }
}

class Monitor extends EventEmitter { 
    constructor(server, database) {
        super();
        
        if (!server) throw new Error("No server provided for monitor.");
        if (!database) throw new Error("No database provided for monitor.");
        
        this.server = server;
        this.database = database;
        this.previous = [];

        setInterval(async () => {
            const warningThresholds = await this.getWarningThresholds();
            const currentServerStats = await Promise.all([
                this.server.type == "Linux" ? this.getNetdataStat("cpu") : this.getWindowsStat("cpu"),
                this.server.type == "Linux" ? this.getNetdataStat("ram") : this.getWindowsStat("ram"),
                this.server.type == "Linux" ? this.getNetdataStat("disk") : this.getWindowsStat("disk"),
                this.getPing()
            ]);

            this.pushToPrevious(currentServerStats);

            let [offlineValues, offlineTime] = [[], 0];

            for (let i = this.previous.length; i !== -1; i --) {
                let current = this.previous[i];
 
                if (current && typeof current == "object") {
                    if (!current?.alive) {
                        offlineValues.push(current);
                    } else {
                        break;
                    }
                }
            }

            if (offlineValues.length > 0) {
                offlineTime = (offlineValues[offlineValues.length - 1].time - offlineValues[0].time) * -1;
            }

            if (offlineTime > 0) {
                const timeoutThreshold = warningThresholds.filter(threshold => threshold.type == "timeout")?.[0];

                if (timeoutThreshold) {    
                    if (offlineTime > 5000) console.debug(`Offline for ${offlineTime}`);
                    if (offlineTime > timeoutThreshold.threshold * 1000) {
                        console.debug("Emitting timeout warning");
                        this.emit("warning", { threshold: timeoutThreshold, current: { type: "timeout" } })
                    }
                } else {
                    console.debug(`No timeout threshold found for ${this.server.hostname}!`);
                }
            }

            if (currentServerStats) {
                const validServerStats = currentServerStats.filter(result => !!result && !!result?.values);

                this.emit("update", validServerStats);
    
                for (let index = 0; index < validServerStats.length; index ++) {
                    let currentStat = validServerStats[index];
                    let thresholdForStat = warningThresholds.filter(threshold => threshold.type == currentStat.type)?.[0];

                    if (thresholdForStat) {
                        if (currentStat.values) {
                            try {
                                let average = await this.averageDataOverTime(60000, currentStat.type, currentStat.type == "ping" ? "ping" : "percentage");
                                
                                if (average) {
                                    console.debug(`Current average ${currentStat.type} for ${this.server.hostname}: ${average}`);

                                    if (average >= thresholdForStat.threshold) {
                                        this.emit("warning", { server: this.server, current: currentStat, threshold: thresholdForStat })
                                    }
                                }
                            } catch (error) {
                                console.debug(error)
                                console.debug(`Error while comparing stats for ${this.server.hostname}`);
                            }
                        } else {
                            console.debug(`Unable to find values for ${this.server.hostname}`);
                        }
                    } else {
                        if (currentStat.type !== "ping") {
                            console.debug(`Unable to find thresholds for type ${currentStat.type} for ${this.server.hostname}!`);
                        }    
                    } 
                }
            }

        }, 5000);
    }
    
    averageDataOverTime(time, type, value) {
        let [data, average, points] = [[], 0, 0];

        for (let i = this.previous.length; i !== -1; i --) {
            let current = this.previous[i];

            if (current) {
                data.push(current);
    
                if (this.previous[this.previous.length - 1].time - current.time >= time) {
                    break;
                }
            }
        }

        data.forEach(dataPoint => {
            if (dataPoint.data) {
                dataPoint.data.forEach(dataValue => {
                    if (dataValue.type == type && dataValue.values && dataValue.values[value]) {
                        points ++;
                        average += dataValue.values[value];
                    }
                })
            }
        })

        return points > 1 ? average / points : null;
    }

    pushToPrevious(serverStats) {
        let previousData = { time: Date.now(), data: serverStats ? serverStats : null };

        if (this.previous.length == 250) this.previous.shift();
        if (typeof serverStats == "object") {
            let ping = serverStats.filter(stat => stat?.type == "ping")?.[0];

            if (ping && ping?.values?.alive) {
                previousData["alive"] = true;
            } else {
                previousData["alive"] = false;
            }
        } else {
            previousData["alive"] = false;
        }

        this.previous.push(previousData);
    }

    getWarningThresholds() {
        return new Promise((Resolve, Reject) => {
            if (this.database.active) {
                this.database.query(`SELECT * FROM warningThresholds WHERE server = '${this.server.address}'`).then(result => {
                    result = JSON.parse(JSON.stringify(result));

                    if (result) {
                        Resolve(result);
                    } else {
                        Reject(`No results found for ${this.server.address}` );
                    } 
                }).catch(error => console.debug(error));
            } else {
                Reject({ "error": "No active database" });
            }
        })
    }

    getActiveWarnings() {
        return new Promise((Resolve, Reject) => {
            if (this.database.active) {
                this.database.query(`SELECT * FROM warnings WHERE server = '${this.server.address}' AND active = 1`).then(result => Resolve(JSON.parse(JSON.stringify(result)))).catch(error => console.debug(error));
            } else {
                Reject({ "error": "No active database" });
            }
        })
    }

    createWarning(data) {
        const { threshold } = data;
        const warningId = crypto.randomBytes(8).toString("hex");
        const warningMessages = {
            "cpu": `The CPU usage on ${this.server.hostname} has exceeded ${threshold.threshold}%`,
            "disk": `The disk usage on ${this.server.hostname} has exceeded ${threshold.threshold}%!`,
            "ram": `The RAM usage on ${this.server.hostname} has exceeded ${threshold.threshold}%!`,
            "timeout": `${this.server.hostname} is offline!`
        }
        
        this.database.query(`INSERT INTO \`warnings\` (server, notify, last_notified, expires, notified_count, type, priority, text, uuid, active) VALUES ('${this.server.address}', ${threshold.notify}, '${Date.now()}', '${Date.now() + (threshold.expiresAfter * 60000)}', 0, '${threshold.type}', ${threshold.priority}, '${warningMessages[threshold.type]}', '${warningId}', 1);`).catch(error => console.debug(error));

        if (threshold.notify) {
            // Notify
        }
    }

    getWindowsStat(name, originalResolve, retryNumber = 0) {
        return new Promise((Resolve, Reject) => {
            axios.get(`http://${this.server.address}:9926/?json=1`).then(response => {
                response = response?.data;
    
                if (typeof response == "object") {
                    const required = ["ramPercent", "cpuPercent", "diskPercent", "ramTotal", "ramUsed", "ramAvailable", "diskTotal", "diskUsed", "diskAvailable"]
    
                    if (required.every(entry => Object.keys(response).includes(entry))) {
                        switch(name) {
                            case "cpu":
                                const { cpuPercent } = response;
                                Resolve({ type: "cpu", values: cpuPercent ? { usage: cpuPercent, percentage: cpuPercent } : null });

                                break;
                            case "ram":
                                const { ramUsed, ramTotal, ramPercent } = response;
                                
                                if (ramUsed && ramTotal && ramPercent) {
                                    Resolve({ type: "ram", values: { usage: ramUsed, total: ramTotal, percentage: ramPercent }});
                                } else {
                                    Resolve({ type: "ram", values: null })
                                }

                                break;
                            case "disk":
                                const { diskTotal, diskUsed, diskPercent } = response;

                                if (diskTotal && diskUsed && diskPercent) {
                                    Resolve({ type: "disk", values: { usage: diskUsed, total: diskTotal, percentage: diskPercent }});
                                } else {
                                    Resolve({ type: "disk", values: null });
                                }

                                break;
                            default:
                                Reject("Invalid stat type!");
                                break;
                        }
                    } else {
                        setTimeout(() => this.getWindowsStat(name, originalResolve ? originalResolve : Resolve, retryNumber + 1), 1000);
                    }
                } else {
                    setTimeout(() => this.getWindowsStat(name, originalResolve ? originalResolve : Resolve, retryNumber + 1), 1000);
                }
            }).catch(error => {
                if (error.code == "ETIMEDOUT" || error.code == "ECONNREFUSED") {
                    console.debug(`[${error.code}] Error while establishing a connection to the windows statistics api on ${this.server.hostname}!`);
                } else {
                    console.debug(`[${error.code}] Encountered an unknown error while connecting to windows statistics api on ${this.server.hostname}!`);
                }

                Resolve({ type: name, values: null });
            })
        })
    }

    getNetdataStat(context, originalResolve, retryNumber = 0) {
        return new Promise((Resolve, Reject) => {
            const contexts = {
                "cpu": "system.cpu",
                "ram": "system.ram",
                "disk": "disk.space"
            }

            if (originalResolve) Resolve = originalResolve;
            if (!contexts[context]) throw new Error(`Invalid context ${context}`);
            if (retryNumber >= 3) return originalResolve({ type: context, values: null });

            axios.get(`http://${this.server.address}:19999/api/v2/data?points=300&format=json2&time_group=average&time_resampling=0&after=${(Date.now() - 5000) / 1000}&before=${(Date.now() + 5000) / 1000}&group_by[0]=dimension&group_by_label[0]=&aggregation[0]=avg&options=jsonwrap|nonzero|flip|ms|jw-anomaly-rates|minify&contexts=*&scope_contexts=${contexts[context]}&scope_nodes=*&nodes=*&instances=*&dimensions=*&labels=`).then(response => {
                response = response?.data;

                if (response && response?.result) {
                    response = mapResultLabelsToData(response.result);

                    switch (context) {
                        case "cpu":
                            let usage = 0;

                            Object.keys(response).forEach(key => {
                                if (typeof response[key] == "object" && typeof response[key]?.[0] == "number") {
                                    usage += response[key][0];
                                }
                            })
            
                            Resolve({ type: "cpu", values: { usage, percentage: usage } });
                            break;
                        case "ram":
                            if (["free", "used", "cached", "buffers"].every(required => Object.keys(response).includes(required))) {
                                const totalRAM = Number(((response["free"][0] + response["used"][0] + response["cached"][0] + response["buffers"][0]) / 1024).toString().slice(0, 4));
                                const usedRAM = Number(((response["used"][0] + response["buffers"][0]) / 1024).toString().slice(0, 4));
                
                                Resolve({ type: "ram", values: { usage: usedRAM, total: totalRAM, percentage: (usedRAM / totalRAM) * 100 }});
                            } else {
                                console.debug("Invalid values for RAM response, retrying..");
                                setTimeout(() => this.getNetdataRAM(originalResolve ? originalResolve : Resolve, retryNumber + 1), 1000);
                            }
                            break;
                        case "disk":
                            if (["reserved for root", "avail", "used"].every(required => Object.keys(response).includes(required))) {
                                const totalDiskSpace = Number((response["reserved for root"][0] + response["avail"][0] + response["used"][0]).toString().slice(0, 4));
                                const usedDiskSpace = Number((response["reserved for root"][0] + response["used"][0]).toString().slice(0, 4));
                
                                Resolve({ type: "disk", values: { usage: usedDiskSpace, total: totalDiskSpace, percentage: (usedDiskSpace / totalDiskSpace) * 100 }});
                            } else {
                                console.debug("Invalid values for disk response, retrying..");
                                setTimeout(() => this.getNetdataDisk(originalResolve ? originalResolve : Resolve, retryNumber + 1), 1000);
                            }
                            break;
                    }
                } else {
                    console.debug(`Invalid values for ${context} response, retrying..`);
                    setTimeout(() => this.getNetdataRAM(originalResolve ? originalResolve : Resolve, retryNumber + 1), 1000);
                }
            }).catch(error => {
                if (error.code == "ETIMEDOUT" || error.code == "ECONNREFUSED") {
                    console.debug(`[${error.code}] Error while establishing a connection to the netdata api on ${this.server.hostname}!`);
                } else {
                    console.debug(`[${error.code}] Encountered an unknown error while connecting to netdata api on ${this.server.hostname}!`);
                }

                Resolve({ type: context, values: null });
            })
        })
    }

    async getPing() {
        const probe = await ping.promise.probe(this.server.address, { timeout: 10 }).catch(() => console.debug(`Error during ping to ${this.server.hostname}`));
        return { type: "ping", values: probe ? { ping: probe.time, alive: probe.alive } : { ping: -1, alive: false } };
    }
}

class Database extends EventEmitter {
    constructor(address, auth, name) {
        super();
        
        this.active = false;
        this.database = null;

        this.connection = sql.createConnection({
            host: address.split(":")[0],
            port: parseInt(address.split(":")[1]),
            user: auth ? auth.split(":")[0] : null,
            password: auth ? auth.split(":")[1] : null,
            database: name
        })

        this.connection.on("error", error => {
            if (error.fatal) {
                console.debug("A fatal error has been detected in the database connection, retrying..\n", error);
    
                this.active = false;
                this.database = null;
    
                setTimeout(() => this.connect(), 500);
            } else {
                console.debug(error.toString());
            }
        })
    }

    waitForConnection() {
        return new Promise((Resolve, Reject) => {
            const start = Date.now();
            const checkInterval = setInterval(() => {
                if (this.active) {
                    Resolve(this.connection);
                    clearInterval(checkInterval);
                } else if (Date.now() - start > 30000) {
                    Reject("Timed out database connection");
                }
            })
        })
    }

    query(string) {
        return new Promise(async (Resolve, Reject) => {
            await this.waitForConnection();

            this.connection.query(string, (error, result) => {
                if (!error) return Resolve(result);
                Reject(error);
            })
        })
    }

    connect() {
        this.connection.connect(error => {
            if (!error) {
                this.emit("connected");
                this.active = true;
                this.database = this.connection;
            } else {
                console.debug("Error while connecting to DB, retrying..\n", error);
                setTimeout(() => this.connect(), 1000);
            }
        });
    }
}

const database = new Database(process.env["DB_HOST"], process.env["DB_AUTH"], "main");

database.connect();
database.query("SELECT * FROM servers").then(results => {
    results = JSON.parse(JSON.stringify(results));

    for (result of results) {
        if (result.monitor) {
            console.log(`Starting monitor for ${result.hostname}`)

            const server = new Server(result.address, result.hostname, result.type, result.timeout);
            const monitor = new Monitor(server, database);

            monitor.on("update", data => {
                const update = data.reduce((obj, value) => {
                    if (value.type) {
                        obj[value.type] = value.values;
                        return obj;
                    }
                }, {})

                if (update.ping && update.ping.alive) {
                    console.debug(`Sending an alive update for ${server.hostname}`);
                    Object.keys(update).forEach(key => {
                        switch (key) {
                            case "cpu":
                                database.query(`UPDATE serverData SET cpu = ${update[key].usage} WHERE serverID = '${server.address}'`);
                                break;
                            case "ram":
                                database.query(`UPDATE serverData SET ramUsed = ${update[key].usage} WHERE serverID = '${server.address}'`);
                                database.query(`UPDATE serverData SET ramTotal = ${update[key].total} WHERE serverID = '${server.address}'`);
                                break;
                            case "disk":
                                database.query(`UPDATE serverData SET hddUsed = ${update[key].usage} WHERE serverID = '${server.address}'`);
                                database.query(`UPDATE serverData SET hddTotal = ${update[key].total} WHERE serverID = '${server.address}'`);
                                break;
                            case "ping":
                                database.query(`UPDATE serverData SET ping = ${update[key].ping} WHERE serverID = '${server.address}'`);
                                database.query(`UPDATE serverData SET online = ${update[key].ping !== -1 ? "1" : "0"} WHERE serverID = '${server.address}'`);
                                break;
                        }
                    })
                } else {
                    console.debug(`Sending an unalive update for ${server.hostname}`);
                    database.query(`UPDATE serverData SET ping = ${update[key].ping ?? "-1"} WHERE serverID = '${server.address}'`);
                    database.query(`UPDATE serverData SET online = 0 WHERE serverID = '${server.address}'`);
                }
            })

            monitor.on("warning", async data => {
                const { threshold } = data;
                const warnings = await monitor.getActiveWarnings();

                if (warnings.length > 0) {
                    for (warning of warnings) {
                        if (warning.type == data.current.type) {
                            database.query(`UPDATE warnings SET expires='${Date.now() + (threshold.expiresAfter * 60000)}' WHERE uuid='${warning.uuid}';`).then(() => {
                                console.debug(`Successfully sent update query for warning ${warning.uuid}!`);
                            }).catch(error => {
                                console.debug(`Error while sending update for warning ${warning.uuid}`, error);
                            })
                        }
                    }
                } else {
                    monitor.createWarning(data);
                }
            })

            monitor.on("error", error => console.debug(error));
        }
    }
})

const warningExpiryInterval = setInterval(async () => {
    database.query(`SELECT * FROM warnings WHERE active = 1`).then(results => {
        results = JSON.parse(JSON.stringify(results));

        results.forEach(result => {
            const { expires, uuid } = result;

            if (expires && Date.now() - Number(expires) > 0) {
                database.query(`UPDATE warnings SET active = 0 WHERE uuid = '${uuid}'`).then(() => {
                    console.debug(`Successfully set warning with UUID ${uuid} to inactive`);
                }).catch(error => {
                    console.debug(`There was an error while setting a warning with the UUID of ${uuid} to inactive.`, error);
                })
            }
        })
    })
}, 1000)

process.on("uncaughtException", error => console.log(error));
process.on("unhandledRejection", error => console.log(error));