import unittest
import json
# from mysqlutils import SQL_runner


class TMB_DAO:

    def __init__(self, stub=False):
        self.is_stub=stub

    def delete_all_msg_timestamp(self, current_time, timestamp):
        pass   

    def insert_message_batch( self, batch ):
        """
        Insert a batch of messages

        :param batch: a string that represent a JSON array of docs
        :type batch: str
        :return: Number of successful insertions
        :rtype: int
        """
        try:
            array = json.loads( batch )
        except Exception as e:
            print(e)
            return -1

        if self.is_stub:
            return len(array)

        # Do things with dependencies: query MySQL, Mongo...

        return -1


class TMBTest( unittest.TestCase ):

    batch = """[ {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111840,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"WIND FARM BALTIC1NW\",\"VesselType\":\"Undefined\",\"Length\":60,\"Breadth\":60,\"A\":30,\"B\":30,\"C\":30,\"D\":30},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.572602,11.929218]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0,\"CoG\":298.7,\"Heading\":203},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257961000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.00316,12.809015]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0.2,\"CoG\":225.6,\"Heading\":240},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111923,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"BALTIC2 WINDFARM SW\",\"VesselType\":\"Undefined\",\"Length\":8,\"Breadth\":12,\"A\":4,\"B\":4,\"C\":4,\"D\":8},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257385000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.219403,13.127725]},\"Status\":\"Under way using engine\",\"RoT\":25.7,\"SoG\":12.3,\"CoG\":96.5,\"Heading\":101},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290} ]"""

    def test_insert_message_batch_interface_1( self  ):
        """
        Function `insert_message_batch` takes a string as an input.
        """
        tmb = TMB_DAO(True) 
        inserted_count = tmb.insert_message_batch( self.batch )
        self.assertTrue( type(inserted_count) is  int and inserted_count >=0) 

    def test_insert_message_batch_interface_2( self  ):
        """
        Function `insert_message_batch` fails nicely if input is not JSON parsable.
        """
        tmb = TMB_DAO(True) 
        array = json.loads( self.batch )
        inserted_count = tmb.insert_message_batch( array )
        self.assertEqual( inserted_count, -1)  


    def test_delete_all_msg_timestamp_1( self, current_time, timestamp ):      
        """
        Function `test_delete_all_msg_timestamp` takes a string as an input.
        """

if __name__ == '__main__':
    unittest.main()