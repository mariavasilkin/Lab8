package main

import (
	"bytes"
	"html/template"
	"log"
	"net/http"
	"os"

	"github.com/shopify/sarama"
	"github.com/gorilla/websocket"
)

// map used for keeping a list of clients where broadcasts need to be sent
var clients = make(map[*websocket.Conn]bool)

// channel used to pass messages that need to be broadcast
var broadcast = make(chan []byte)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}
var podName string

var (
	newline = []byte{'\n'}
	space   = []byte{' '}
)

func init() {
	podName = os.Getenv("podname")
	if podName == "" {
		podName = "Unknown Pod Name"
	}
}

func main() {
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/ws", handleWSConnections)
	go handleMessages()
	err := http.ListenAndServe(":8000", nil)
	if err != nil {
		log.Fatalln(err)
	}
}

// Handler for the root path. It merely returns the formatted content of the
// homePage template declared at the end of this file. The only dynamic bit of that
// template is the pod name
func serveHome(w http.ResponseWriter, r *http.Request) {
	podstr := struct {
		PodName string
	}{PodName: podName}
	homePage.Execute(w, podstr)
}

// Handler for the websockets connections
func handleWSConnections(w http.ResponseWriter, r *http.Request) {

	// upgrade the connection to a websocket
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer ws.Close()

	//create an instance of a SyncProducer
	producer, err := sarama.NewSyncProducer([]string{"kafka"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	// keep track of the clients in order to be able to send broadcasts
	clients[ws] = true

	// endless loop that reads from the websocket and writes them to the broadcast channel
	for {
		_, text, err := ws.ReadMessage()
		if err != nil {
			log.Printf("error: %v", err)
			delete(clients, ws)
			break
		}
		text = bytes.TrimSpace(bytes.Replace(msg, newline, space, -1))

		msg := &sarama.ProducerMessage{Topic: podName, Value: sarama.StringEncoder(text)}
		_, _, err := producer.SendMessage(msg)
		if err != nil {
			log.Fatal(err)
		}
		//broadcast <- msg
	}
}

// Handler that listens to messages coming from the broadcast channel and
// sends them to each one of the websocket clients.
// We should probably implement some sanitation here to prevent injecting malicious code to clients.
func handleMessages() {
	consumer, err := sarama.NewConsumer([]string{"kafka"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
  
	partitionConsumer, err := consumer.ConsumePartition(podName, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal(err)
	}
	defer partitionConsumer.Close()

	for {
		msg := <-partitionConsumer.Messages()
		for client := range clients {
			err := client.WriteMessage(websocket.TextMessage, msg.Value)
			if err != nil {
				log.Printf("error: %v", err)
				client.Close()
				delete(clients, client)
			}
		}
	}
}

// Template to render a UI. Then only variable is the Pod Name.
// Tested only on Chrome.
// Adapted from https://github.com/gorilla/websocket/blob/master/examples/chat/home.html
var homePage = template.Must(template.New("").Parse(`
<!DOCTYPE html>
<html lang="en">
<head>
<title>Connected to Pod {{.PodName}}</title>
<script type="text/javascript">
window.onload = function () {
    var conn;
    var msg = document.getElementById("msg");
    var log = document.getElementById("log");

    function appendLog(item) {
        var doScroll = log.scrollTop > log.scrollHeight - log.clientHeight - 1;
        log.appendChild(item);
        if (doScroll) {
            log.scrollTop = log.scrollHeight - log.clientHeight;
        }
    }

    document.getElementById("form").onsubmit = function () {
        if (!conn) {
            return false;
        }
        if (!msg.value) {
            return false;
        }
        conn.send(msg.value);
        msg.value = "";
        return false;
    };

    if (window["WebSocket"]) {
        conn = new WebSocket("ws://" + document.location.host + "/ws");
        conn.onclose = function (evt) {
            var item = document.createElement("div");
            item.innerHTML = "<b>Connection closed.</b>";
            appendLog(item);
        };
        conn.onmessage = function (evt) {
            var messages = evt.data.split('\n');
            for (var i = 0; i < messages.length; i++) {
                var item = document.createElement("div");
                item.innerText = messages[i];
                appendLog(item);
            }
        };
    } else {
        var item = document.createElement("div");
        item.innerHTML = "<b>Your browser does not support WebSockets.</b>";
        appendLog(item);
    }
};
</script>
<style type="text/css">
html {
    overflow: hidden;
}

body {
    overflow: hidden;
    padding: 0;
    margin: 0;
    width: 100%;
    height: 100%;
    background: gray;
}

#log {
    background: white;
    margin: 0;
    padding: 0.5em 0.5em 0.5em 0.5em;
    position: absolute;
    top: 2.5em;
    left: 0.5em;
    right: 0.5em;
    bottom: 3em;
    overflow: auto;
}

#form {
    padding: 0 0.5em 0 0.5em;
    margin: 0;
    position: absolute;
    bottom: 1em;
    left: 0px;
    width: 100%;
    overflow: hidden;
}

</style>
</head>
<body>
<div>
<p style="text-align:center">Chatting on {{.PodName}}</p>
</div>
<div id="log"></div>
<form id="form">
    <input type="submit" value="Send" />
    <input type="text" id="msg" size="64"/>
</form>
</body>
</html>
`))
