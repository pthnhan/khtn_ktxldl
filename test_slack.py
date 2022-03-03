import slack

slackclient = slack.WebClient(token = "xoxb-2515891084886-2737529751366-WsL48eOcrS1bNQr9ys67NP4p")
slackclient.files_upload(channels='#status', file = "slippage.zip")
