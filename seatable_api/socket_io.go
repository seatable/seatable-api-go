package seatable_api

import (
	"fmt"
	"github.com/graarh/golang-socketio"
	"github.com/graarh/golang-socketio/transport"
	"time"
)

type SocketIO struct {
	Client *gosocketio.Client
	Base   *SeaTableAPI
}

type Message struct {
	msg interface{}
}

func InitSocketIO(base *SeaTableAPI) (*SocketIO, error) {
	url := base.DtableServerURL + "?dtable_uuid=" + base.DtableUUID

	c, err := gosocketio.Dial(
		url,
		transport.GetDefaultWebsocketTransport())
	if err != nil {
		err := fmt.Errorf("failed to dial socket io: %v", err)
		return nil, err
	}

	return &SocketIO{Client: c, Base: base}, nil
}

func (sio *SocketIO) Connect() error {
	err := sio.Client.On(gosocketio.OnConnection, func(c *gosocketio.Channel) {
		if time.Now().Unix() >= sio.Base.JwtExp {
			err := sio.Base.Auth(false)
			if err != nil {
				err := fmt.Errorf("failed to auth: %v", err)
				panic(err)
			}
			fmt.Println(time.Now(), "[ SeaTable SocketIO JWT token refreshed ]")
		}
		var data []string
		data = append(data, sio.Base.DtableUUID)
		data = append(data, sio.Base.JwtToken)
		c.Emit(JOIN_ROOM, data)
		fmt.Println(time.Now(), "[ SeaTable SocketIO connection established ]")
	})
	if err != nil {
		return err
	}

	err = sio.Client.On(gosocketio.OnDisconnection, func(c *gosocketio.Channel) {
		fmt.Println(time.Now(), "[ SeaTable SocketIO connection dropped ]")
	})
	if err != nil {
		return err
	}

	err = sio.Client.On("/connect_error", func(h *gosocketio.Channel, args Message) {
		fmt.Println(time.Now(), "[ SeaTable SocketIO connection error ]", args.msg)
	})
	if err != nil {
		return err
	}

	err = sio.Client.On(UPDATE_DTABLE, func(h *gosocketio.Channel, args Message) {
		fmt.Println(time.Now(), "[ SeaTable SocketIO on UPDATE_DTABLE ]")
		fmt.Println(args)
	})
	if err != nil {
		return err
	}

	err = sio.Client.On(NEW_NOTIFICATION, func(h *gosocketio.Channel, args Message) {
		fmt.Println(time.Now(), "[ SeaTable SocketIO on NEW_NOTIFICATION ]")
		fmt.Println(args)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sio *SocketIO) On(method string, f interface{}) error {
	return sio.Client.On(method, f)
}
