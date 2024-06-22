const express = require('express');
const axios = require('axios');
const bodyParser = require('body-parser');
const fs = require('fs');


const app = express();
app.use(express.static(__dirname + '/public'));
app.use(bodyParser.urlencoded({
  extended: true
}));
const port = 8080;
app.listen(port, () => console.log(`listening on port ${port}!`));

// Serve HTML
app.get('/', (req, res) => {
  res.sendFile('./public/home.html', {
    root: __dirname
  });
});


app.post('/input', (req, res) => {
  var link = 'https://api.memegen.link/images/custom/'+req.body.top+'/'+req.body.bottom+'.png?background='+req.body.imageUrl;
  var response = {
    status  : 200,
    success : 'Updated Successfully',
    url : link
  }

  res.end(JSON.stringify(response));
  return response;
});

app.get('/result', (req, res) => {
  res.sendFile('./public/result.html', {
    root: __dirname
  });
});

app.get('/error', (req, res) => {
  res.sendFile('./public/error.html', {
    root: __dirname
  });
});
