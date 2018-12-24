package core

import (
	log "github.com/sirupsen/logrus"
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
)

var (
	DbName                             = "rma_subscribes"
	TableName_SubscribeTunnelRealFund  = "SubscribeTunnelRealFund"
	TableName_SubscribeCorpHoldMon     = "SubscribeCorpHoldMon"
	TableName_SubscribeQuoteMon        = "SubscribeQuoteMon"
	TableName_SubscribeCustRisk        = "SubscribeCustRisk"
	TableName_SubscribeCustHold        = "SubscribeCustHold"
	TableName_SubscribeCustGroupHold   = "SubscribeCustGroupHold"
	TableName_SubscribeProuctGroupRisk = "SubscribeProuctGroupRisk"
	TableName_SubscribeNearDediveHold  = "SubscribeNearDediveHold"
)

func CreateDBAndTableAfterDrop(session *r.Session, shardsAndReplicas int) error {
	res, err := r.DBList().Run(session)
	if err != nil {
		return err
	}
	defer res.Close()

	var rows []string
	err = res.All(&rows)
	if err != nil {
		return err
	}

	existed := false
	for _, d := range rows {
		if d == DbName {
			existed = true
			break
		}
	}
	if existed {
		log.Infof("DB %s existed, drop it", DbName)
		res, err := r.DBDrop(DbName).Run(session)
		if err != nil {
			log.Errorf("DB %s drop failed", DbName)
			return err
		}
		res.Close()
	}

	err = createDb(session, DbName)
	if err != nil {
		return err
	}
	session.Use(DbName)

	primaryKey := "ActionKey"
	err = createTable(session, TableName_SubscribeTunnelRealFund, primaryKey, shardsAndReplicas)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeCorpHoldMon, primaryKey, shardsAndReplicas)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeQuoteMon, primaryKey, shardsAndReplicas)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeCustRisk, primaryKey, shardsAndReplicas)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeCustHold, primaryKey, shardsAndReplicas)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeCustGroupHold, primaryKey, shardsAndReplicas)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeProuctGroupRisk, primaryKey, shardsAndReplicas)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeNearDediveHold, primaryKey, shardsAndReplicas)
	if err != nil {
		return err
	}

	return nil
}

func CreateDBAndTableIfNotExisted(session *r.Session, shardsAndReplicas int) error {
	err := createDbIfNotExisted(session, DbName)
	if err != nil {
		log.Error(err)
		return err
	}
	session.Use(DbName)

	primaryKey := "ActionKey"
	err = createTableIfNotExisted(session, TableName_SubscribeTunnelRealFund, primaryKey, shardsAndReplicas)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeCorpHoldMon, primaryKey, shardsAndReplicas)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeQuoteMon, primaryKey, shardsAndReplicas)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeCustRisk, primaryKey, shardsAndReplicas)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeCustHold, primaryKey, shardsAndReplicas)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeCustGroupHold, primaryKey, shardsAndReplicas)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeProuctGroupRisk, primaryKey, shardsAndReplicas)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeNearDediveHold, primaryKey, shardsAndReplicas)
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

func createDbIfNotExisted(session *r.Session, dbName string) error {
	res, err := r.DBList().Run(session)
	if err != nil {
		return err
	}
	defer res.Close()

	var rows []string
	err = res.All(&rows)
	if err != nil {
		return err
	}

	existed := false
	for _, d := range rows {
		if d == dbName {
			existed = true
			break
		}
	}
	if !existed {
		res, err := r.DBCreate(dbName).Run(session)
		if err != nil {
			return err
		}
		res.Close()
		log.Infof("DB %s created.", dbName)

	} else {
		log.Infof("DB %s existed.", dbName)
	}

	return nil
}

func createTableIfNotExisted(session *r.Session, tableName string, primaryKey string, shardsAndReplicas int) error {
	res, err := r.TableList().Run(session)
	if err != nil {
		return err
	}
	defer res.Close()

	var rows []string
	err = res.All(&rows)
	if err != nil {
		return err
	}

	existed := false
	for _, t := range rows {
		if t == tableName {
			existed = true
			break
		}
	}
	if !existed {
		res, err := r.TableCreate(tableName, r.TableCreateOpts{PrimaryKey: primaryKey, Shards: shardsAndReplicas, Replicas: shardsAndReplicas}).Run(session)
		if err != nil {
			return err
		}
		res.Close()
		log.Infof("Table %s created.", tableName)

	} else {
		log.Infof("Table %s existed.", tableName)
	}

	resDel, errDel := r.Table(tableName).Delete(r.DeleteOpts{Durability: "hard", ReturnChanges: false}).Run(session)
	if errDel != nil {
		log.Errorf("Clear data for table %s : %v", tableName, errDel)
		return errDel
	}
	resDel.Close()
	log.Infof("%s data clear successed", tableName)
	return nil
}

func createDb(session *r.Session, dbName string) error {
	res, err := r.DBCreate(dbName).Run(session)
	if err != nil {
		log.Errorf("DB %s created failed : %v", dbName, err)
		return err
	}
	res.Close()
	log.Infof("DB %s created.", dbName)

	return nil
}

func createTable(session *r.Session, tableName string, primaryKey string, shardsAndReplicas int) error {
	res, err := r.TableCreate(tableName, r.TableCreateOpts{PrimaryKey: primaryKey, Shards: shardsAndReplicas, Replicas: shardsAndReplicas}).Run(session)
	if err != nil {
		log.Errorf("Table %s created failed : %v", tableName, err)
		return err
	}
	res.Close()
	log.Infof("Table %s created.", tableName)

	return nil
}
