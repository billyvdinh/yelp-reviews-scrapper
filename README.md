## Deployment guide.

1. ###### Clone repository from github.

    `git clone  https://github.com/mbpup/sls-yelp-scrapper.git`
    
2. ###### Create virtualenv

    `virtualenv env`
    
3. ###### install requirements

    `pip install -r requirements.txt`

4. ###### Install serverless dependencies
    
    `npm install`
    
5. ###### Create config file
    ```json
    {
        "REGION": "deployment region here",
        "REDSHIFT_HOST": "redshift hostname here",
        "REDSHIFT_PORT": "port number here",
        "REDSHIFT_DBNAME": "database name here",
        "REDSHIFT_USERNAME": "database username here",
        "REDSHIFT_PASSWORD": "database password here"
    }
    ```

6. ###### Deploy service

    `sls deploy -v --stage {stage}`
    
    
    
### **Enjoy!**