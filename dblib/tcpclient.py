from socket import *

serverName = "localhost"
serverPort = 3000
clientSocket = socket(AF_INET, SOCK_STREAM)
clientSocket.connect((serverName, serverPort))
sentence = input()
clientSocket.send(sentence.encode())
clientSocket.close()