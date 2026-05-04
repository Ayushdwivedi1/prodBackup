this is my curl
postman request POST 'http://localhost:9016/topology/lsp/getFilterLspData' \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer YOUR_TOKEN_HERE' \
  --body '["UBL-J104-P-T3-SR"]'

below is my response
[
    {
        "links": [
            {
                "start": "UBL-J104-P-T3-SR",
                "end": "UBL-BNG-J960-PE-T1-SR",
                "relation": "LSP",
                "lspName": "UBL-J104-UBL-J960",
                "metaInfo": {
                    "mplsLspPrimaryTimeUp": "3727792",
                    "mplsLspState": "2",
                    "mplsLspOctets": "3232197061",
                    "mplsPathProperties": "5",
                    "mplsPathCos": "255",
                    "mplsLspPathUtilization": "86.1919",
                    "mplsPathExplicitRoute": "0",
                    "mplsPathType": "2",
                    "mplsPathBandwidth": "100000",
                    "mplsLspTimeUp": "745902706",
                    "mplsLspAge": "745920558",
                    "mplsPathSetupPriority": "7",
                    "mplsLspPathChanges": "1941",
                    "mplsPathExclude": "0",
                    "mplsLspStandbyPaths": "0",
                    "mplsPathRecordRoute": "0",
                    "mplsLspOperationalPaths": "1",
                    "mplsPathHoldPriority": "0",
                    "mplsPathInclude": "4",
                    "percentageUpTime": "99.9976",
                    "mplsLspConfiguredPaths": "1",
                    "mplsLspLastPathChange": "3728063",
                    "mplsLspLastTransition": "745902949",
                    "mplsLspTransitions": "1",
                    "mplsLspPackets": "9310986"
                }
            },
            {
                "start": "UBL-BNG-J960-PE-T1-SR",
                "end": "UBL-J104-P-T3-SR",
                "relation": "LSP",
                "lspName": "UBL-MX960-UBL-MX104",
                "metaInfo": {
                    "mplsLspPrimaryTimeUp": "261808889",
                    "mplsLspState": "2",
                    "mplsLspOctets": "23168549086",
                    "mplsPathProperties": "5",
                    "mplsPathCos": "255",
                    "mplsLspPathUtilization": "61.7828",
                    "mplsPathExplicitRoute": "0",
                    "mplsPathType": "2",
                    "mplsPathBandwidth": "1000000",
                    "mplsLspTimeUp": "944052697",
                    "mplsLspAge": "944506454",
                    "mplsPathSetupPriority": "5",
                    "mplsLspPathChanges": "1199",
                    "mplsPathExclude": "0",
                    "mplsLspStandbyPaths": "0",
                    "mplsPathRecordRoute": "0",
                    "mplsLspOperationalPaths": "1",
                    "mplsPathHoldPriority": "5",
                    "mplsPathInclude": "4",
                    "percentageUpTime": "99.952",
                    "mplsLspConfiguredPaths": "1",
                    "mplsLspLastPathChange": "63661667",
                    "mplsLspLastTransition": "745905379",
                    "mplsLspTransitions": "3",
                    "mplsLspPackets": "19736479"
                }
            }
        ],
        "nodes": [
            {
                "neId": "172.31.31.45",
                "pmEmsId": "172.31.31.45",
                "fmEmsId": "172.31.31.45",
                "ipv4": "172.31.31.45",
                "neName": "UBL-BNG-J960-PE-T1-SR",
                "hostname": "UBL-BNG-J960-PE-T1-SR",
                "friendlyName": "UBL-BNG-J960-PE-T1-SR",
                "neType": "ROUTER",
                "neCategory": "ROUTER",
                "neStatus": "READY",
                "domain": "TRANSPORT",
                "vendor": "JUNIPER",
                "technology": "COMMON",
                "geoL1Name": "SOUTH",
                "geoL2Name": "KARNATAKA",
                "geoL3Name": null,
                "geoL4Name": null,
                "latitude": "15.350058",
                "longitude": "75.149313",
                "model": "mx960",
                "serialNumber": "JN123FD5CAFA",
                "macAddress": "0:a0:a5:7b:b8:21",
                "popId": "4170271",
                "popName": "Hubli",
                "popCode": "UBL",
                "lspName": "MAS_J960-UBL-J960-BNG"
            },
            {
                "neId": "172.31.34.89",
                "pmEmsId": "172.31.34.89",
                "fmEmsId": "172.31.34.89",
                "ipv4": "172.31.34.89",
                "neName": "UBL-J104-P-T3-SR",
                "hostname": "UBL-J104-P-T3-SR",
                "friendlyName": "UBL-J104-P-T3-SR",
                "neType": "ROUTER",
                "neCategory": "ROUTER",
                "neStatus": "READY",
                "domain": "TRANSPORT",
                "vendor": "JUNIPER",
                "technology": "COMMON",
                "geoL1Name": "SOUTH",
                "geoL2Name": "KARNATAKA",
                "geoL3Name": null,
                "geoL4Name": null,
                "latitude": "15.350058",
                "longitude": "75.149313",
                "model": "mx104",
                "serialNumber": "AJ359",
                "macAddress": "ec:3e:f7:a0:7a:cf",
                "popId": "4170271",
                "popName": "Hubli",
                "popCode": "UBL",
                "lspName": "UBL-J104-UBL-J960"
            }
        ],
        "timestamp": "2026-03-24T04:05:00Z"
    }
]

when use check this router - UBL-J104-P-T3-SR
so this links is valid means start UBL-J104-P-T3-SR and end UBL-BNG-J960-PE-T1-SR

so, start UBL-BNG-J960-PE-T1-SR" and end UBL-J104-P-T3-SR this come when we give below curl
postman request POST 'http://localhost:9016/topology/lsp/getFilterLspData' \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer YOUR_TOKEN_HERE' \
  --body '[" UBL-BNG-J960-PE-T1-SR"]'

--  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --


Without filter (or filter blank)
Returns links where either end is in selected WEST router set.
So includes:
WEST → WEST
WEST → other-region
other-region → WEST

but i want
WEST → WEST
WEST → EAST
WEST → NORTH
WEST → SOUTH

Becasue this is wrong
other-region → WEST

when user select WEST region WEST TO ANOTER REGION
why we see ANOTHER Region to WEST
