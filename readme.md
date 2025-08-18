## ASKAPTrigger

This package allows you to trigger observations of other telescope automatically based on the ASKAP pointings. To use this package, simply run `pip install .` to install it.

If you want to add more features (e.g., adding more telescopes, sending slack message, etc.), you need to modify/create `LotrunRunner` class under `askaptrigger.py`, and `askaprunner.py` under `scripts` folder accordingly. 

Note: the script will listen to the Ice service, therefore, you need to run the code within the ASKAP network.

### Trigger MWA

copy configuration files to the current working directory by running `askaprunner_setup`. It will copy two files `askap_trigger_config.json`, and `trigger_mwa_config.json`

You need to specify `askap_project_ids` (list of ASKAP project ids to be monitored, e.g., AS114 for all bandpass observations) and `mwa_project_id` (one mwa project id for the trigger, e.g., T001) pairs in `askap_trigger_config.json` file. For each pair, you will give an alias name as the key. If you want to trigger on all ASKAP observations, put `null` for `askap_project_ids`.

For MWA observation parameters, you need to specify them in `trigger_mwa_config.json`. The key is the mwa project id, and the value is a dictionary for the observation parameters for this specific project id. For a given parameter, if you don't specify, it will use the default value based on the trigger service. See [here](https://mwatelescope.atlassian.net/wiki/spaces/MP/pages/24972656/Triggering+web+services) for all allowed parameters. Note - the source coordinate parameter (i.e., `ra` and `dec`) will be updated automatically, so you don't need to specify them.

make sure you have put your secure key for the corresponding mwa project id under `~/.config/mwa_trigger_key.json`. The key is the project id, and the value is the secret key.

run `askaprunner -p <pair_alias_name> [--dryrun]`. The code will launch a tsp job for each observation that is listed in the `askap_project_ids` list (except OdC and Beamform scan) - therefore you could do `export TS_SOCKET=<xxx>` (modify as needed), and `tsp` to check status of the triggers. The code will also create a database (`trigger.db`) in the current working directory. You can also check that database to make sure that everything is being executed as expected.
