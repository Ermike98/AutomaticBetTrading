from mysql import connector
from database_login import username, password, host
from utility.bet_utility import BetPrice


def setup_database():
    create_sql = """CREATE DATABASE `bets`;
    
                    USE `bets`;
                    
                    CREATE TABLE `bet` (
                      `idbet` int NOT NULL AUTO_INCREMENT,
                      `ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                      `match_id` int DEFAULT NULL,
                      `site` varchar(45) DEFAULT NULL,
                      `bet_type` varchar(45) DEFAULT NULL,
                      `back_price` float DEFAULT NULL,
                      `back_size` float DEFAULT NULL,
                      `lay_price` float DEFAULT NULL,
                      `lay_size` float DEFAULT NULL,
                      PRIMARY KEY (`idbet`),
                      KEY `match_key_idx` (`match_id`),
                      CONSTRAINT `match_key` FOREIGN KEY (`match_id`) REFERENCES `match` (`idmatch`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
                    
                    CREATE TABLE `match` (
                      `idmatch` int NOT NULL AUTO_INCREMENT,
                      `club1` varchar(45) DEFAULT NULL,
                      `club2` varchar(45) DEFAULT NULL,
                      `date` date DEFAULT NULL,
                      PRIMARY KEY (`idmatch`),
                      UNIQUE KEY `match_index` (`club1`,`club2`,`date`) /*!80000 INVISIBLE */
                    ) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;
                    
                """
    conn = connector.connect(
        host=host,
        user=username,
        password=password
    )
    c = conn.cursor()
    c.execute(create_sql)
    conn.close()


def add_match(club1, club2, date):
    sql = """INSERT INTO `bets`.`match` (`club1`, `club2`, `date`) 
             VALUES (%s, %s, %s);"""
    val = (club1, club2, date)

    conn = connector.connect(
        host=host,
        user=username,
        password=password,
        database='bets'
    )

    c = conn.cursor()
    c.execute(sql, val)
    conn.commit()
    conn.close()


def add_matches(matches):
    conn = connector.connect(
        host=host,
        user=username,
        password=password,
        database='bets'
    )

    c = conn.cursor()
    sql = f"INSERT INTO `bets`.`match` (`club1`, `club2`, `date`) VALUES {'(%s, %s, %s), ' * len(matches)};"
    val = sum(matches, tuple())
    c.execute(sql, val)
    conn.commit()
    conn.close()


def add_bet(match_tuple: tuple[str, str, str], site: str, sport, bet_type: str, bet_info: BetPrice):
    sql = """INSERT INTO `bets`.`bet` 
    (`match_id`, `site`, `bet_type`, `back_price`, `back_size`, `lay_price`, `lay_size`) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
    match_id = find_match_id(match_tuple)
    if match_id:
        val = (*match_tuple, site, sport, bet_type, *bet_info)
    else:
        val = tuple()
        pass

    conn = connector.connect(
        host=host,
        user=username,
        password=password,
        database='bets'
    )

    c = conn.cursor()
    c.execute(sql, val)
    conn.commit()
    conn.close()


def find_match_id(match_tuple: tuple[str, str, str]):
    sql = """SELECT * FROM bets.match
             where club1 = %s and club2 = %s and date = %s;"""

    conn = connector.connect(
        host=host,
        user=username,
        password=password,
        database='bets'
    )

    c = conn.cursor()
    result = c.execute(sql, match_tuple)
    conn.close()
    return result
