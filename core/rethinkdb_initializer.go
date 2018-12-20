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

func CreateDBAndTableAfterDrop(session *r.Session) error {
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
		_, err = r.DBDrop(DbName).Run(session)
		if err != nil {
			log.Errorf("DB %s drop failed", DbName)
			return err
		}
	}

	err = createDb(session, DbName)
	if err != nil {
		return err
	}
	session.Use(DbName)

	primaryKey := "ActionKey"
	err = createTable(session, TableName_SubscribeTunnelRealFund, primaryKey)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeCorpHoldMon, primaryKey)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeQuoteMon, primaryKey)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeCustRisk, primaryKey)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeCustHold, primaryKey)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeCustGroupHold, primaryKey)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeProuctGroupRisk, primaryKey)
	if err != nil {
		return err
	}
	err = createTable(session, TableName_SubscribeNearDediveHold, primaryKey)
	if err != nil {
		return err
	}

	return nil
}

func CreateDBAndTableIfNotExisted(session *r.Session) error {
	err := createDbIfNotExisted(session, DbName)
	if err != nil {
		log.Error(err)
		return err
	}
	session.Use(DbName)

	primaryKey := "ActionKey"
	err = createTableIfNotExisted(session, TableName_SubscribeTunnelRealFund, primaryKey)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeCorpHoldMon, primaryKey)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeQuoteMon, primaryKey)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeCustRisk, primaryKey)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeCustHold, primaryKey)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeCustGroupHold, primaryKey)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeProuctGroupRisk, primaryKey)
	if err != nil {
		log.Error(err)
		return err
	}
	err = createTableIfNotExisted(session, TableName_SubscribeNearDediveHold, primaryKey)
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
		_, err = r.DBCreate(dbName).Run(session)
		if err != nil {
			return err
		}
		log.Infof("DB %s created.", dbName)

	} else {
		log.Infof("DB %s existed.", dbName)
	}

	return nil
}

func createTableIfNotExisted(session *r.Session, tableName string, primaryKey string) error {
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
		_, err = r.TableCreate(tableName, r.TableCreateOpts{PrimaryKey: primaryKey, Shards: 4, Replicas: 4}).Run(session)
		if err != nil {
			return err
		}
		log.Infof("Table %s created.", tableName)

	} else {
		log.Infof("Table %s existed.", tableName)
	}

	return nil
}

func createDb(session *r.Session, dbName string) error {
	_, err := r.DBCreate(dbName).Run(session)
	if err != nil {
		log.Errorf("DB %s created failed : %v", dbName, err)
		return err
	}
	log.Infof("DB %s created.", dbName)

	return nil
}

func createTable(session *r.Session, tableName string, primaryKey string) error {
	_, err := r.TableCreate(tableName, r.TableCreateOpts{PrimaryKey: primaryKey, Shards: 4, Replicas: 4}).Run(session)
	if err != nil {
		log.Errorf("Table %s created failed : %v", tableName, err)
		return err
	}
	log.Infof("Table %s created.", tableName)

	return nil
}
