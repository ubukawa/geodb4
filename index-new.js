const config = require('config')
const fs = require('fs')
const Queue = require('better-queue')
const { spawn } = require('child_process')
const Parser = require('json-text-sequence').parser
const tilebelt = require('@mapbox/tilebelt')

const srcdb = config.get('srcdb')
const ogr2ogrPath = config.get('ogr2ogrPath')
const tippecanoePath = config.get('tippecanoePath')
const minzoom = config.get('minzoom')
const maxzoom = config.get('maxzoom')
const mbtilesDir = config.get('mbtilesDir')

let keyInProgress = []
let idle = true

const isIdle = () => {
    return idle
}

const noPressureWrite = (downstream, f) => {
    return new Promise((res) => {
        if (downstream.write(`\x1e${JSON.stringify(f)}\n`)) {
            res()
        } else {
            downstream.once('drain', () => {
                res()
            })
        }
    })
}

const fetch = (parser, srcdb, downstream) =>{
    return new Promise((resolve, reject) =>{
        let count = 0
        let features = []
        parser
        .on('data', f => {
            f.tippecanoe = {
                layer: srcdb.layer,
                minzoom: srcdb.minzoom,
                maxzoom: srcdb.maxzoom
            }
            delete f.properties.SHAPE_Length 
            if ((f.properties.contour % 100) == 0){
                f.tippecanoe.minzoom = srcdb.minzoom
            } else if ((f.properties.contour % 40) == 0){
                f.tippecanoe.minzoom = srcdb.minzoom + 1
            } else {
                f.tippecanoe.minzoom = srcdb.minzoom + 2
            }
            count++
            if (f) features.push(f)
            //downstream.write(`\x1e${JSON.stringify(f)}\n`)
        })
        .on('error', err => {
            console.error(err.stack)
            reject()
        })
        .on('finish', async () => {
            for (f of features) {
                try {
                    await noPressureWrite(downstream, f)
                } catch (e) {
                    reject(e)
                }
            }
            resolve(count)
        })
    })
}


const dumpAndModify = async(downstream, tile) => {
    return new Promise((resolve, reject) =>{
        //from here
        const parser = new Parser()
        const [z, x, y] = tile
        const bbox = tilebelt.tileToBBOX([x, y, z])     
        var ogr2ogr = spawn(ogr2ogrPath, [
            '-f', 'GeoJSONSeq',
            '-lco', 'RS=YES',
            '/vsistdout/',
        //    '-clipdst', 0, 52.4827, 5.625, 55.76573,
            '-clipdst', bbox[0], bbox[1], bbox[2], bbox[3],
            srcdb.url
            //`small-data/${key}.geojson`
        ])
        ogr2ogr.on('exit', () => {
            let nowTime = new Date()
            //console.log(`${key}: GDAL reading ends at ${nowTime}:\n`)
        })

        await fetch(parser, srcdb, downstream)
        resolve()

        //ogr2ogr.stdout.pipe(parser)
        //until here
    })
}



const sleep = (wait) => {
    return new Promise((resolve, reject) => {
        setTimeout( () => {resolve()}, wait)
    })
}

const queue = new Queue(async (t, cb) => {
    const startTime = new Date()
    const key = t.key
    const tile = t.tile
    const [z, x, y] = tile
    const tmpPath = `${mbtilesDir}/part-${z}-${x}-${y}.mbtiles`
    const dstPath = `${mbtilesDir}/${z}-${x}-${y}.mbtiles`
    const clipbbox = tilebelt.tileToBBOX([x, y, z])

    keyInProgress.push(key)
    console.log(`[${keyInProgress}] in progress`)
    await sleep(1000)
// conversion
    const tippecanoe = spawn(tippecanoePath, [
        `--output=${tmpPath}`,
        '--no-feature-limit',
        '--no-tile-size-limit',
        '--force',
        '--simplification=2',
        `--clip-bounding-box=${clipbbox.join(',')}`, 
        '--quiet',
        `--minimum-zoom=${minzoom}`,
        `--maximum-zoom=${maxzoom}`
    ], { stdio: ['pipe', 'inherit', 'inherit']})
       .on('exit', () => {
            fs.renameSync(tmpPath, dstPath)
            const endTime = new Date()
            console.log(`${key}: Tippecanoe ${startTime.toISOString()} --> ${endTime.toISOString()} (^o^)/`)
            keyInProgress = keyInProgress.filter((v) => !(v === key))
            return cb()
        })
    while(!isIdle()){
        await sleep(5000)
    }
    try {
        await dumpAndModify(tippecanoe.stdin, tile)
    } catch(e) {
        cb(true)
    }
    tippecanoe.stdin.end()
},{
    concurrent: config.get('concurrent'),
    maxRetries: config.get('maxRetries'),
    retryDelay: config.get('retryDelay')
})


const queueTasks = () => {
    //for (let key of ['bndl1', 'bndl2', 'bndl3', 'bndl4', 'bndl5', 'bndl6']){
    for (let tile of srcdb.tiles){
        const key = `${tile[0]}-${tile[1]}-${tile[2]}`
    queue.push({
            key: key,
            tile: tile
        })
    }
}

const shutdown = () => {
    console.log('shutdown (^_^)')
    process.exit(0)
}

const main = async () =>{
    queueTasks()
    queue.on('drain', () => {
        shutdown()
    })
}

main()