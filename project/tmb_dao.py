import unittest
import json

# # from mysqlutils import SQL_runner
# import sys, re
# from mysqlutils import SQL_runner


class TMB_DAO:

    def __init__(self, stub=False):
        self.is_stub=stub

    def delete_all_msg_timestamp(self, batch):
        """
        Insert a batch of messages

        :param batch: a string that represent a JSON array of docs
        :type batch: str
        :return: Number of successful deletions
        :rtype: int
        """
        if batch == "" or batch == None:
            return -1 
        
        try:
            array = json.loads( batch )
        except Exception:
            return -1

        if self.is_stub:
            return len(array)

        # Do things with dependencies: query MySQL, Mongo...

        return -1  

    def insert_message_batch( self, batch ):
        """
        Insert a batch of messages

        :param batch: a string that represent a JSON array of docs
        :type batch: str
        :return: Number of successful insertions
        :rtype: int
        """
        if batch == "" or batch == None:
            return -1 

        try:
            array = json.loads( batch )
        except Exception:
            return -1

        if self.is_stub:
            return len(array)

        return -1

    def insert_message( self, batch ):
        """
        Insert an AIS message

        :param batch: a string that represent a JSON array of docs
        :type batch: str
        :return: completion code number
        :rtype: int
        """
        if batch == "" or batch == None:
            return -1 

        try:
            array = json.loads( batch )
        except Exception:
            return -1

        if self.is_stub:
            return len(array)

        return -1

    def read_most_recent_ship_pos( self, batch ):
        """
        Insert a batch of messages

        :param batch: a string that represent a JSON array of docs
        :type batch: str
        :return: list of ship documents
        :rtype: list
        """
        if batch == "" or batch == None:
            return -1 

        try:
            array = json.loads( batch )
        except Exception:
            return -1

        if self.is_stub:
            return array

        return -1    

    def read_all_ports( self, batch ):
        """
        Insert a batch of messages

        :param batch: a string that represent a JSON array of docs
        :type batch: str
        :return: list of Port documents
        :rtype: list
        """
        if batch == "" or batch == None:
            return -1 

        try:
            array = json.loads( batch )
        except Exception:
            return -1

        if self.is_stub:
            return array

        return -1  

    def read_all_ship_pos_scale3( self, batch ):
        """
        Insert a batch of messages

        :param batch: a string that represent a JSON array of docs
        :type batch: str
        :return: list of (Port or Position) documents 
        :rtype: list
        """
        if batch == "" or batch == None:
            return -1 

        try:
            array = json.loads( batch )
        except Exception:
            return -1   

        if self.is_stub:
            return array

        return -1    

    def read_last_5_pos( self, batch ):
        """
        Insert a batch of messages

        :param batch: a string that represent a JSON array of docs
        :type batch: str
        :return: a single document 
        :rtype: dict
        """
        if batch == "" or batch == None:
            return -1 

        try:
            array = json.loads( batch )
        except Exception:
            return -1

        if self.is_stub:
            return array[0]

        return -1

    def read_position_to_port_id(self, batch):
        """
        Read most recent positions of ships headed to port with given ID

        :param batch: a string that represent a JSON array of docs
        :type batch: str
        :return: a list of position documents
        :rtype: list
        """
        if batch == "" or batch == None:
            return -1

        try:
            array = json.loads( batch )
        except Exception:
            return -1

        if self.is_stub:
            return array

        return -1

    def read_position_given_port(self, batch):
        """
        Read most recent positions of ships headed to given port

        :param batch: a string that represent a JSON array of docs
        :type batch: str
        :return: a list of position documents or array of port documents
        :rtype: list
        """
        if batch == "" or batch == None:
            return -1

        try:
            array = json.loads( batch )
        except Exception:
            return -1

        if self.is_stub:
            return array

        return -1

    def find_tiles_zoom_2(self, batch):
        """
        Given a background map tile for zoom level 1 (2), find the 4 tiles of zoom level 2 (3) that are contained in it

        :param batch: a string that represent a JSON array of docs
        :type batch: str
        :return: a list of map tile description documents
        :rtype: list
        """
        if batch == "" or batch == None:
            return -1

        try:
            array = json.loads( batch )
        except Exception:
            return -1

        if self.is_stub:
            return array

        return -1




class TMBTest( unittest.TestCase ):
    batch = """[ {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
                {\"Timestamp\":\"2020-11-19T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111840,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"WIND FARM BALTIC1NW\",\"VesselType\":\"Undefined\",\"Length\":60,\"Breadth\":60,\"A\":30,\"B\":30,\"C\":30,\"D\":30},
                {\"Timestamp\":\"2020-11-20T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.572602,11.929218]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0,\"CoG\":298.7,\"Heading\":203},
                {\"Timestamp\":\"2020-11-21T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257961000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.00316,12.809015]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0.2,\"CoG\":225.6,\"Heading\":240},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111923,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"BALTIC2 WINDFARM SW\",\"VesselType\":\"Undefined\",\"Length\":8,\"Breadth\":12,\"A\":4,\"B\":4,\"C\":4,\"D\":8},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257385000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.219403,13.127725]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":12.3,\"CoG\":96.5,\"Heading\":101},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290} ]"""

    def test_insert_message_batch_interface_1( self ):
        """
        Function `insert_message_batch` takes a JSON parsable string as an input.
        Returns: number (int) of insertions
        """
        tmb = TMB_DAO(True) 
        inserted_count = tmb.insert_message_batch( self.batch )
        self.assertTrue(type(inserted_count) is int and inserted_count >=0)

    def test_insert_message_batch_interface_2( self ):
        """
        Function `insert_message_batch` fails nicely if input is not JSON parsable, or is empty.
        """
        tmb = TMB_DAO(True) 
        array = json.loads( self.batch )
        inserted_count = tmb.insert_message_batch( array )
        self.assertEqual(inserted_count, -1)  

    def test_insert_message_interface_1( self ):
        """
        Function `insert_message` takes a JSON parsable string as an input.
        Returns: completion code number
        """
        tmb = TMB_DAO(True) 
        sucess_code_num = tmb.insert_message( self.batch )
        self.assertTrue(type(sucess_code_num) is int)

    def test_insert_message_interface_2( self ):
        """
        Function `insert_message_batch` fails nicely if input is not JSON parsable, or is empty.
        """
        tmb = TMB_DAO(True) 
        array = json.loads( self.batch )
        sucess_code_num = tmb.insert_message( array )
        self.assertEqual((sucess_code_num), -1)  

    def test_delete_all_msg_timestamp_1( self ):      
        """
        Function `delete_all_msg_timestamp` takes a JSON parsable string as an input.
        Returns: number (int) of deletions
        """
        tmb = TMB_DAO(True)
        deletion_count = tmb.delete_all_msg_timestamp( self.batch )
        self.assertTrue(type(deletion_count) is int)

    def test_delete_all_msg_timestamp_2( self ):
        """
        Function `delete_all_msg_timestamp` fails nicely if input is not JSON parsable, or is empty.
        """
        tmb = TMB_DAO(True) 
        array = json.loads( self.batch )
        deletion_count = tmb.delete_all_msg_timestamp( array )
        self.assertEqual(deletion_count, -1)

    def test_read_most_recent_ship_pos1( self ):
        """
        Function `read_most_recent_ship_pos` takes a JSON parsable string as an input.
        Returns: list of ship documents
        """
        tmb = TMB_DAO(True)
        ships = tmb.read_most_recent_ship_pos( self.batch )
        self.assertTrue(type(ships) is list)

    def test_read_most_recent_ship_pos2( self ):
        """
        Function `read_most_recent_ship_pos` fails nicely if input is not JSON parsable, or is empty.
        """
        tmb = TMB_DAO(True) 
        array = json.loads( self.batch )
        ships = tmb.read_most_recent_ship_pos( array )
        self.assertEqual(ships, -1)  

    def test_read_all_ports1( self ):
        """
        Function `read_all_ports` takes a JSON parsable string as an input.
        Returns: list of Port documents
        """
        tmb = TMB_DAO(True)
        ships = tmb.read_most_recent_ship_pos( self.batch )
        self.assertTrue(type(ships) is list)

    def test_read_all_ports2( self ):
        """
        Function `read_all_ports` fails nicely if input is not JSON parsable, or is empty.
        """
        tmb = TMB_DAO(True) 
        array = json.loads( self.batch )
        ships = tmb.read_most_recent_ship_pos( array )
        self.assertEqual(ships, -1) 

    def test_all_ship_pos_scale3_1( self ):
        """
        Function `read_all_ship_pos_scale3` takes a JSON parsable string as an input.
        Returns: list of (Port or Position) documents
        """
        tmb = TMB_DAO(True)
        documents = tmb.read_all_ship_pos_scale3( self.batch )
        self.assertTrue(type(documents) is list)

    def test_all_ship_pos_scale3_2( self ):
        """
        Function `read_all_ship_pos_scale3` fails nicely if input is not JSON parsable, or is empty.
        """
        tmb = TMB_DAO(True) 
        array = json.loads( self.batch )
        documents = tmb.read_all_ship_pos_scale3( array )
        self.assertEqual(documents, -1)   

    def test_read_last_5_pos1( self ):
        """
        Function `read_last_5_pos` takes a JSON parsable string as an input.
        Returns: a single document (dict)
        """
        tmb = TMB_DAO(True)
        document = tmb.read_last_5_pos( self.batch )
        self.assertTrue(type(document) is dict)

    def test_read_last_5_pos2( self ):
        """
        Function `read_last_5_pos` fails nicely if input is not JSON parsable, or is empty.
        """
        tmb = TMB_DAO(True) 
        array = json.loads( self.batch )
        document = tmb.read_last_5_pos( array )
        self.assertEqual(document, -1)

    def test_read_position_to_port_id1(self):
        """
        Function 'read_position_to_port_id' takes a JSON parsable string as an input.
        Returns: a list of documents
        """
        tmb = TMB_DAO(True)
        document = tmb.read_position_to_port_id(self.batch)
        self.assertTrue(type(document) is list)

    def test_read_position_to_port_id2(self):
        """
        Function `read_position_to_port_id` fails nicely if input is not JSON parsable, or is empty.
        """
        tmb = TMB_DAO(True)
        array = json.loads(self.batch)
        document = tmb.read_position_to_port_id(array)
        self.assertEqual(document, -1)

    def test_read_position_given_port1(self):
        """
        Function 'read_position_given_port' takes a JSON parsable string as an input.
        Returns: a list of documents
        """
        tmb = TMB_DAO(True)
        document = tmb.read_position_given_port(self.batch)
        self.assertTrue(type(document) is list)

    def test_read_position_given_port2(self):
        """
        Function `read_position_given_port` fails nicely if input is not JSON parsable, or is empty.
        """
        tmb = TMB_DAO(True)
        array = json.loads(self.batch)
        document = tmb.read_position_given_port(array)
        self.assertEqual(document, -1)

    def test_find_tiles_zoom_2_1(self):
        """
        Function 'find_tiles_zoom_2' takes a JSON parsable string as an input.
        Returns: a list of documents
        """
        tmb = TMB_DAO(True)
        document = tmb.find_tiles_zoom_2(self.batch)
        self.assertTrue(type(document) is list)

    def test_find_tiles_zoom_2_2(self):
        """
        Function `find_tiles_zoom_2` fails nicely if input is not JSON parsable, or is empty.
        """
        tmb = TMB_DAO(True)
        array = json.loads(self.batch)
        document = tmb.find_tiles_zoom_2(array)
        self.assertEqual(document, -1)


if __name__ == '__main__':
    unittest.main()