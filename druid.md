## cattle-druid integration

This file contains documentation of the integration between cattle and druid. This integration has major advantages above using the current Web interface of cattle, since this depends on nginx timeout for responses and outdated POST multipart uploads from the client. These two are specifically critical for large files.

### TODO
- Update this instructions when error code reportings in druid become ready
- Update this instructions when druid's webhooks become ready
- update this instructions when building JSON schemas through druid becomes ready

### Workflow

This is the workflow to trigger cattle conversions from druid without using cattle's Web interface, **with no support for webhooks** (yet):

1. Go to https://druid.datalegend.net/ and log in, click on your profile, *My account*
2. You need to make sure you belong to an organization where the user *Cattle* is also a member. The way to do this is to click on your profile, *User settings*, *My organizations*, and add yourselves **and** the user *Cattle* as members of that organization. Just in case, I have added you to the [testorggg](https://druid.datalegend.net/testorggg) organization already (which meets the conditions)
2. Select an existing dataset (or create a new one); make sure its access level is set to *Public*
3. In the dataset main page, go to *Files* on the vertical left menu
4. Click on *Upload files* and select both your CSV and JSON schema file
5. After the upload is complete, open a new tab in your browser and go to the URL http://cattle.datalegend.net/druid/user/dataset, where *user* is your druid username, and *dataset* is the name of the dataset where you have uploaded the CSV and JSON schema files
6. Wait for the conversion to finish. If it succeeds, your converted Linked Data will become available in the *Graphs* section of your dataset's page. Notice the timestamps in the graph names; if the conversion went well, you'll see a relatively recent date in those.

That's it! There are two major drawbacks

- There is no monitoring of long conversion processes
- If the conversion fails, you won't notice anything

The current workaround for these is that you launch the process, wait for a day, and look at your *Graphs* list to discover changes. If those don't show up, please let me know (via mail/slack) and I'll look into it.
