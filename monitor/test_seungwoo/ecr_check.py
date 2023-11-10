import boto3, re, os
import json
from datetime import datetime, timezone, timedelta
import sys

def printItem(element, depth):
    if type(element) == type(dict()):
        for key, item in element.items():
            print('\t' * depth, end='')
            print(key, end='')
            if (type(item) == type(dict())):
                print()
                printItem(item, depth + 1)
            elif (type(item) == type(list())):
                for i in range(len(item)):
                    print()
                    printItem(item[i], depth + 1)
            else:
                print(":", item)
    elif type(element) == type(list()):
        for elem in element:
            printItem(elem, depth)
    else:
        print('\t' * depth, end='')
        print(element)

def printImage(client, repository):
    images = client.describe_images(
        registryId=repository['registryId'],
        repositoryName=repository['repositoryName']
    )
    print(f"---- IMAGES in repository : {repository['repositoryName']} ----")
    printItem(images, 0)


if __name__ == "__main__":
    sys.stdout = open('out.txt', 'w')

    session = boto3.Session()

    client = session.client('ecr')
    response = client.describe_repositories()
    repositories = response['repositories']
    printItem(response, 0)
    for repository in repositories:
        print()
        printImage(client, repository)

    sys.stdout.close()