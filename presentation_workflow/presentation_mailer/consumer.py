import json
import pika
from pika.exceptions import AMQPConnectionError
import django
import os
import sys
import time
from django.core.mail import send_mail


sys.path.append("")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "presentation_mailer.settings")
django.setup()

while True:
    try:

        def process_approvals(ch, method, properties, body):
            content = json.loads(body)
            send_mail(
                "You presentation has been accepted",
                f"{content['presenter_name']}, we're happy to tell you that your presentation {content['title']} has been accepted",
                "admin@conference.go",
                [content["presenter_email"]],
                fail_silently=False,
            )
            print("  Received %r" % body)

        def process_rejections(ch, method, properties, body):
            content = json.loads(body)
            send_mail(
                "You presentation has been rejected",
                f"{content['presenter_name']}, we regret to inform you that your presentation {content['title']} has been rejected",
                "admin@conference.go",
                [content["presenter_email"]],
                fail_silently=False,
            )
            print("  Received %r" % body)

        parameters = pika.ConnectionParameters(host="rabbitmq")
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue="presentation_approvals")
        channel.queue_declare(queue="presentation_rejections")
        channel.basic_consume(
            queue="presentation_approvals",
            on_message_callback=process_approvals,
            auto_ack=True,
        )
        channel.basic_consume(
            queue="presentation_rejections",
            on_message_callback=process_rejections,
            auto_ack=True,
        )
        channel.start_consuming()
    except AMQPConnectionError:
        print("Could not connect to RabbitMQ")
        time.sleep(2.0)
