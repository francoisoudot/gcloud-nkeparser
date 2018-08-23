/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload and metadata.
 * @param {!Function} callback Callback function to signal completion.
 */

// Payload example: [{"timestamp":1533564208,"device_id":"1B2DEB","temperature":19.1},{"timestamp":1533563008,"device_id":"1B2DEB","temperature":23.5},{"timestamp":1533561808,"device_id":"1B2DEB","temperature":23.5}]
// JSON base-64 encoded: W3sidGltZXN0YW1wIjoxNTMzNTY0MjA4LCJkZXZpY2VfaWQiOiIxQjJERUIiLCJ0ZW1wZXJhdHVyZSI6MTkuMX0seyJ0aW1lc3RhbXAiOjE1MzM1NjMwMDgsImRldmljZV9pZCI6IjFCMkRFQiIsInRlbXBlcmF0dXJlIjoyMy41fSx7InRpbWVzdGFtcCI6MTUzMzU2MTgwOCwiZGV2aWNlX2lkIjoiMUIyREVCIiwidGVtCnBlcmF0dXJlIjoyMy41fV0=
//
// functions call nkeParser --data='{ "data":"YzkwMDAyNTQwY2JmMDBlYjAwZWIwMGVi","attributes": {"device":"1B2DEB","time":"1533564208","deviceType":"nke_model"}}'

// Dependencies call
const PubSub = require('@google-cloud/pubsub');
const Buffer = require('safe-buffer').Buffer;

// ENV variables for PubSub
const projectId = 'iot-test-212508';
const keyFilename = './iot-test-212508-b0b52a0361c5.json';
const topicName = 'timeseries_big-query';

// START PubSub
const pubsub = new PubSub({
  projectId: projectId,
  keyFilename: keyFilename
});

//---------------------------------------
// START nkeparser functions
exports.nkeparser = (event, callback) => {
  const pubsubMessage = event.data;
  // Extract payload and parse it
  var payload = Buffer.from(pubsubMessage.data, 'base64').toString();
  var attributes = pubsubMessage.attributes;
  console.log('entry data');
  console.log(payload);
  console.log(attributes);
  var payloadParsed = decodePayload(payload, attributes);
  console.log('end function');
  callback();
};

//---------------------------------------
// END nkeparser functions
//---------------------------------------

//Function to decode payload
function decodePayload(payload, attributes) {
  //Main
  var data = payload;
  var attributes = attributes;
  var device_id = attributes.device;
  var time = attributes.time;
  var mode = data.slice(2, 4);
  let res;
  console.log('decoding');
  console.log(data);
  console.log(time);
  console.log(device_id);

  switch (mode) {
    case '00': // example values 1st frame - c90002540cbf00eb00eb00eb
      console.log('case00');
      var temp_T0 = new Temp(parseInt(time), device_id, data.slice(10, 14));
      var temp_T0_20 = new Temp(time - 20 * 60, device_id, data.slice(14, 18));
      var temp_T0_40 = new Temp(time - 40 * 60, device_id, data.slice(18, 22));

      // Need to manage value storage for the LSB_temp_T0_60
      // /!\ This data coding is not recommended
      // var LSB_temp_T0_60 = data.slice(22, 24);

      //Publish results
      res = [temp_T0, temp_T0_20, temp_T0_40];
      console.log(temp_T0);
      console.log(res);
      queueMessage(res, attributes);
      break;

    case '10': // example values 2nd frame -
      // Need to manage value storage from first payload
      // /!\ This data coding is not recommended
      // var LSB_temp_T0_60 = payload.data.body.LSB_temp_T0_60;
      // var temp_T0_60 = new Temp(
      //   time - 60 * 60,
      //   LSB_temp_T0_60.concat(data.slice(4, 6))
      // );
      console.log('case10');

      var temp_T0_80 = new Temp(time - 80 * 60, data.slice(6, 10));
      var temp_T0_100 = new Temp(time - 100 * 60, data.slice(10, 14));
      //Return results
      res = { timeseries: true, temp_T0_80, temp_T0_100 };
      return res;
      break;

    case '01': // example values etendues - c901000006e800ed00e400
      console.log('case01');

      var temp_moy = new Temp(time, data.slice(10, 14));
      var temp_max = new Temp(time, data.slice(14, 18));
      var temp_min = new Temp(time, data.slice(18, 22));

      //Return results
      res = { timeseries: true, temp_moy, temp_max, temp_min };
      return res;
      break;

    case '02': //alarmes - c9020001 alarm on - c9020000 alarm off
      console.log('case02');

      var alarm = data.slice(7, 8) == '1'; //boolean

      //Return results
      res = { timeseries: false, alarm: alarm };
      return res;
      break;

    case '03': //value systeme - c9030024d80103000000
      console.log('case03');

      var battery = parseInt('0x'.concat(data.slice(6, 8))) / 10; //voltage of battery

      //Return results
      res = { timeseries: false, battery: battery };
      return res;
      break;
  }

  //Temperature formating function
  function Temp(time, device_id, temp) {
    this.timestamp = time;
    this.device_id = device_id;
    this.temperature = parseTemp(temp);
  }

  //Temperature parsing function
  function parseTemp(temp) {
    return (
      parseInt('0x'.concat(temp.slice(2, 4).concat(temp.slice(0, 2)))) / 10
    );
  }
}

// ----- PUBSUB - TO BE SPLIT IN ANOTHER LIBRARY -----
//Function to getTopic or createTopic if it doesn't exist - called by queueMessage
function getTopic(cb) {
  pubsub.createTopic(topicName, (err, topic) => {
    // topic already exists.
    if (err && err.code === 6) {
      cb(null, pubsub.topic(topicName));
      return;
    }
    cb(err, topic);
  });
}

// Function to publish the payload on the device type topic
function queueMessage(data, attributes) {
  // Defines the data and the attributes of the PubSub message
  var data = JSON.stringify(data);
  var attributes = attributes;
  console.log(data);
  getTopic((err, topic) => {
    if (err) {
      console.log('Error occurred while getting pubsub topic', err);
      return;
    }

    const publisher = topic.publisher();
    publisher.publish(Buffer.from(data), attributes, err => {
      if (err) {
        console.log('Error occurred while queuing background task', err);
      } else {
        console.log(`Message queued for background processing`);
      }
    });
  });
}
