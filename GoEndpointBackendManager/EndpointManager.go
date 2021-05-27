package GoEndpointBackendManager

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	etcdv3 "go.etcd.io/etcd/client/v3"
)

// Quan ly cac endpoint dua tren 1 duong dan goc
// etcdServer dia chi cua ectdserver (vi du : http://127.0.0.1:2379)
// etcClient etcd client dung de call toi etcd server
type EndPointManager struct {
	mux sync.Mutex
	// etcdServer   string
	etcdBasePath string
	endPoints    []*EndPoint
	// etcdApi      etcdclient.KeysAPI
	etcdClient      *etcdv3.Client
	etcdEnpoints    []string
	defaultEnpoints sync.Map
}

func (e *EndPointManager) SetDefaultEnpoint(ServiceID string, host string, port string, epType TType) {
	ep := &EndPoint{
		Host:      host,
		Port:      port,
		ServiceID: ServiceID + "/" + epType.String() + ":" + host + ":" + port,
		Type:      epType,
	}
	e.defaultEnpoints.Store(ServiceID+"/"+epType.String(), ep)
}

func (e *EndPointManager) GetDefaultEndpoint(ServiceID string, epType TType) (*EndPoint, error) {
	val, ok := e.defaultEnpoints.Load(ServiceID + "/" + epType.String())
	if !ok {
		return nil, errors.New("Can not find enpoint")
	}
	ep := val.(*EndPoint)
	return ep, nil
}

// GetEndPoint get random endpoint from endpoints
func (e *EndPointManager) GetEndPoint() (error, *EndPoint) {
	if e.endPoints != nil && len(e.endPoints) == 0 {
		return errors.New("Not found any endpoint with serviceid " + e.etcdBasePath), nil
	}
	for i := 0; i < len(e.endPoints); i++ {
		if e.endPoints[i].IsGoodEndpoint() {
			return nil, e.endPoints[i]
		}
	}
	return errors.New("Not found any endpoint is running"), nil
}

// GetEndPoints get all endpoint from etcd and base path
func (e *EndPointManager) GetEndPoints() (error, []*EndPoint) {
	if e.endPoints == nil {
		return errors.New("Enpoind nil"), nil
	}
	return nil, e.endPoints
}

func (e *EndPointManager) GetEndPointType(t TType) (error, *EndPoint) {
	for i := 0; i < len(e.endPoints); i++ {
		if e.endPoints[i].Type == t && e.endPoints[i].IsGoodEndpoint() {
			return nil, e.endPoints[i]
		}
	}
	return errors.New("Can not found endpoint service"), nil
}

// LoadEndpoint load all endpoint from etcd and base path
func (e *EndPointManager) LoadEndpoints() error {

	// log.Println("Load endpoint from", e.etcdEnpoints, "with base path", e.etcdBasePath)

	return e.doLoadEndpoint()
}

func (e *EndPointManager) LoadEndPointFromServer(etcdServer, basePath string) error {
	// e.etcdServer = etcdServer
	e.etcdBasePath = basePath
	return e.doLoadEndpoint()
}

type etcdClient struct {
	endpoints []string
	client    *etcdv3.Client
}

var etcdclient *etcdClient

func GetEtcdClient(endpoints []string) (*etcdClient, error) {
	if etcdclient != nil {
		return etcdclient, nil
	}
	cfgv3 := etcdv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}
	aClient, err := etcdv3.New(cfgv3)
	if err != nil {
		return nil, err
	}
	etcdclient = &etcdClient{
		endpoints: endpoints,
		client:    aClient,
	}
	return etcdclient, nil

}

func (e *EndPointManager) doLoadEndpoint() error {

	aClient, err := GetEtcdClient(e.etcdEnpoints)
	if err != nil {
		return err
	}

	e.etcdClient = aClient.client
	opts := []etcdv3.OpOption{etcdv3.WithPrefix()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	res, err := e.etcdClient.Get(ctx, e.etcdBasePath, opts...)
	cancel()
	if err != nil {
		return err
	}

	var listEp []*EndPoint
	for _, kv := range res.Kvs {
		epPath := string(kv.Key)
		// epValue := string(kv.Value)
		// log.Println("enpoint path key :", epPath, "enpoint value :", epValue)
		err, ep := e.parseEndpoint(epPath)
		if err != nil {
			log.Println(err.Error())
		} else {
			listEp = append(listEp, ep)
		}
	}
	if len(listEp) > 0 {
		e.replaceAll(listEp)
	}
	return nil

}

func (e *EndPointManager) parseEndpoint(endPointPath string) (error, *EndPoint) {
	var ep EndPoint
	baseNode := strings.Split(endPointPath, "/")
	if len(baseNode) == 0 {
		return errors.New("Parse endpoint error " + endPointPath), nil
	}
	nodeName := baseNode[len(baseNode)-1]
	token := strings.Split(nodeName, ":")
	if len(token) != 3 {
		return errors.New("Parse endpoint error " + nodeName), nil
	}
	port := token[2]

	ep.Type = StringToTType(token[0])
	ep.Host = token[1]
	ep.Port = port
	ep.ServiceID = endPointPath
	return nil, &ep
}

func (e *EndPointManager) removeEndPoint(ep *EndPoint) {
	for i, v := range e.endPoints {
		if v.Host == ep.Host && v.Port == ep.Port && v.Type == ep.Type {
			e.endPoints = append(e.endPoints[:i], e.endPoints[i+1:]...)
			return
		}
	}
}

func (e *EndPointManager) replaceAll(listEndPoints []*EndPoint) {
	e.mux.Lock()
	defer e.mux.Unlock()
	e.endPoints = listEndPoints
}

func (e *EndPointManager) EventChangeEndPoints(fn FncProcessEventChange) {
	if e.etcdClient == nil {
		return
	}
	opts := []etcdv3.OpOption{etcdv3.WithPrefix()}
	watchChan := e.etcdClient.Watch(context.Background(), e.etcdBasePath, opts...)
	for wresp := range watchChan {
		for _, ev := range wresp.Events {
			if ev.Type == etcdv3.EventTypePut {
				// log.Println("co su kien thay doi ", e.etcdBasePath)
				err, ep := e.parseEndpoint(string(ev.Kv.Key))
				if err != nil {
					log.Println(err.Error())
					continue
				}
				if !ep.IsGoodEndpoint() {
					continue
				}
				fn(ep)
			}
		}
	}

}

func NewEndPointManager(aEndpoints []string, ServiceID string) EndPointManagerIf {
	epm := &EndPointManager{
		etcdEnpoints: aEndpoints,
		etcdBasePath: ServiceID,
	}
	err := epm.LoadEndpoints()
	if err != nil {
		log.Println("New endpoint manager of service:", ServiceID, "err", err)
	}
	return epm
}
