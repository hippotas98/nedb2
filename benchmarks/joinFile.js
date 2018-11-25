const fs = require('fs');
const outFile = fs.createWriteStream('./insert.file', {flags: `a`});


const file = () => {
    for(let iter = 1;iter<=4;++iter){
        let path = './insert' + iter + '.bench.db'
        let readFile = fs.createReadStream(path)
                            .on('data', (chunk) => {
                                    outFile.write(chunk)
                            })
                            .on('error', (err) => {
                                console.log(err);
                            })
                            .on('end', () => {
                                console.log('End')
                            })


    }
}
file();