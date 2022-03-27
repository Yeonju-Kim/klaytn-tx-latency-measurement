const fs = require('fs')
const Caver = require('caver-js')
const axios = require('axios')
const path = require('path')
var parquet = require('parquetjs-lite');
const AWS = require('aws-sdk');
const moment = require('moment');

const sleep = (ms) => {
    return new Promise(resolve=>{
        setTimeout(resolve,ms)
    })
}

async function uploadToS3(data) {
    const s3 = new AWS.S3();

    const filename = await makeParquetFile(data)

    const param = {
        'Bucket':process.env.S3_BUCKET,
        'Key':filename,
        'Body':fs.createReadStream(filename),
        'ContentType':'application/octet-stream'
    }

    await s3.upload(param, function(err, data) {
        if(err) {
            throw err
        }
        console.log('s3 uploaded.', data)
        fs.unlinkSync(filename)
    })
}

async function makeParquetFile(data) {
    var schema = new parquet.ParquetSchema({
        txhash:{type:'UTF8'},
        startTime:{type:'TIMESTAMP_MILLIS'},
        endTime:{type:'TIMESTAMP_MILLIS'},
        chainId:{type:'INT64'},
        duration:{type:'INT64'}
    })

    var d = new Date()
    //20220101_032921
    var datestring = moment().format('YYYYMMDD_HHMMSS')

    var filename = `${datestring}.parquet`

    // create new ParquetWriter that writes to 'fruits.parquet`
    var writer = await parquet.ParquetWriter.openFile(schema, filename);

    await writer.appendRow(data)

    writer.close()

    return filename;
}

function loadConfig() {
    if(process.env.NODE_ENV === undefined) {
        console.log("using .env")
        require('dotenv').config({path:path.join(__dirname,'.env')})
    } else {
        console.log(`using .env.${process.env.NODE_ENV}`)
        require('dotenv').config({path:path.join(__dirname,`.env.${process.env.NODE_ENV}`)})
    }
}

async function sendSlackMsg(msg) {
    axios.post(process.env.SLACK_API_URL, {
        'channel':process.env.SLACK_CHANNEL,
        'mrkdown':true,
        'text':msg
    }, {
        headers: {
            'Content-type':'application/json',
            'Authorization':`Bearer ${process.env.SLACK_AUTH}`
        }
    })
}

async function checkBalance(addr) {
    const caver = new Caver(process.env.CAVER_URL)
    const balance = await caver.rpc.klay.getBalance(addr)
    const balanceInKLAY = caver.utils.convertFromPeb(balance, 'KLAY')

    if(balanceInKLAY < process.env.BALANCE_ALERT_CONDITION_IN_KLAY) {
        sendSlackMsg(`Current balance of <${process.env.SCOPE_URL}/account/${addr}|${addr}> is less than ${process.env.BALANCE_ALERT_CONDITION_IN_KLAY} KLAY! balance=${balanceInKLAY}`)
    }

}

async function sendTx() {
    const caver = new Caver(process.env.CAVER_URL)
    const keyring = caver.wallet.keyring.createFromPrivateKey(process.env.PRIVATE_KEY)

    caver.wallet.add(keyring)

    checkBalance(keyring.address)

	// Create value transfer transaction
	const vt = caver.transaction.valueTransfer.create({
		from: keyring.address,
		to: keyring.address,
		value: 0,
		gas: 25000,
	})

	// Sign to the transaction
	const signed = await caver.wallet.sign(keyring.address, vt)

    const start = new Date().getTime()
	// Send transaction to the Klaytn blockchain platform (Klaytn)
	const receipt = await caver.rpc.klay.sendRawTransaction(signed)
    const end = new Date().getTime()
    const chainId = caver.utils.hexToNumber(signed.chainId)

    const data = {
        chainId: chainId,
        txhash: receipt.transactionHash,
        startTime: start,
        endTime: end,
        duration: end-start
    }
    console.log(`${data.chainId},${data.txhash},${data.startTime},${data.endTime},${data.duration}`)

    uploadToS3(data)
}

async function main() {
    const start = new Date().getTime()
    console.log(`starting tx latency measurement... start time = ${start}`)

    // first send.
    sendTx()

    // run sendTx every 1 min.
    const interval = 60*1000
    setInterval(()=>{
        sendTx()
    }, interval)

    while(1) {
        await sleep(60*1000)
    }
}
loadConfig()
main()