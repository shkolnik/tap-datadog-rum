# tap-datadog-rum

Singer Tap to pull raw event data from Datadog's Real User Monitoring (RUM) system.

* Supports 1 or more streams configured using a Datadog RUM event query (copy and paste from DD search UI).
* Supports extraction of custom fields (from event context) based on a user-configured mapping.
* Infers event schemas using the [Genson](https://github.com/wolverdude/GenSON) library.
* Uses cursor based fetching with cursor stored in Singer state between runs.
* A given run of the tap will end once all existing events have been extracted.

### Example Configuration
```json
{
  "api_key": "DD_API_KEY_SECRET",
  "app_key": "DD_APP_KEY_SECRET",
  "start_date": "2022-08-15T00:00:00Z",
  "streams": {
    "front_end_crashes": {
      "query": "env:production @context.browser_reload_required:true",
      "attribute_mapping": {
        "company_id": "attributes.attributes.context.company_id",
        "error_presentation_style": "attributes.attributes.context.error_presentation_style",
        "error_user_message": "attributes.attributes.context.user_message",
        "team": "attributes.attributes.context.team"
      }
    }
  }
}
```
