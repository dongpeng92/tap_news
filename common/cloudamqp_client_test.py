from cloudamqp_client import CloudAMQPClient

CLOUDAMQP_URL = "amqp://ofmdwnsa:JpyDbMoshiM-rdHgNw2NjoMDnv1A_SDY@fish.rmq.cloudamqp.com/ofmdwnsa"
TEST_QUEUE_NAME = 'test'

def test_basic():
    client = CloudAMQPClient(CLOUDAMQP_URL, TEST_QUEUE_NAME)

    sentMsg = {'test':1234}
    client.sendMessage(sentMsg)
    client.sleep(10)
    receivedMsg = client.getMessage()
    assert sentMsg == receivedMsg
    print 'test_basic passed!'

if __name__ == '__main__':
    test_basic()
