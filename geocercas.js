const fs = require('fs')
const util = require('util')
const mysql = require('mysql2/promise')
require('dotenv').config()

const db_config = {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: 'yap_sandbox'
}

const read_file_async = util.promisify(fs.readFile)

const json_file_path = './arroz_paisa.json'

async function read_and_store_data() {
    const connection = await mysql.createConnection(db_config)

    try {
        const jsonFileContent = await read_file_async(json_file_path, 'utf8')
        const jsonData = JSON.parse(jsonFileContent)

        await connection.query('START TRANSACTION')

        const [shops] = await connection.execute('SELECT id, nombre, idRestaurante FROM tiendas WHERE idRestaurante = 115 AND id = 157') || [null]

        for (const shop of shops) {
            for (const geocerca of jsonData.features) {
                const match = geocerca.properties?.description.match(/(\d+)/)
                const name = geocerca.properties?.name
                const price = match ? parseInt(match[0], 10) : null

                const [result_geo] = await connection.execute('INSERT INTO geocercas (idRestaurante, nombre) VALUES (?, ?)', [shop?.idRestaurante, name])

                for (const [index, points] of geocerca.geometry?.coordinates[0].entries()) {
                    await connection.execute('INSERT INTO geocercasPuntos (idGeocerca, posicion, longitud, latitud) VALUES (?,?,?,?)', [result_geo.insertId, index + 1, points[0], points[1]])
                }

                await connection.execute('INSERT INTO tiendasGeocercas (idGeocerca, idTienda, valor) VALUES (?,?,?)', [result_geo.insertId, shop?.id, price])
            }
        }

        await connection.query('COMMIT')

    } catch (error) {
        await connection.query('ROLLBACK')
        console.error('Error:', error)
    } finally {
        connection.end()
    }
}

read_and_store_data()
