def cenas_main() -> int:
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
        handlers=[
            logging.FileHandler("JPay2Zenith.log"),
            logging.StreamHandler(sys.stdout)
        ]
        , encoding='utf-8', level=logging.INFO)

    logging.info("Current locale: "+str(locale.getlocale(category=locale.LC_NUMERIC)))

    try:
        locale.setlocale(locale.LC_NUMERIC, "en_us")
    except:
        logging.critical("Could not configure Locale en_us")
        return 1
        
    logging.info("New locale: "+str(locale.getlocale(category=locale.LC_NUMERIC)))

    parser = ArgumentParser()
    parser.add_argument("-f", "--file", dest="file", required=True,
        help="JPay Settlement Filename")
        
    parser.add_argument("-o", "--out", dest="out", required=False,
        help="Zenith Output Filename")

    parser.add_argument("-a", "--aggid", dest="aggid", required=False, default="Jumia0001",
        help="JPay Aggregator Id assigned by Zenith, default Jumia0001")

    args = parser.parse_args()
    
    path = Path(args.file)

    if not path.is_file():
        logging.critical('The file does not exists or is a directory: '+args.file)
        return 1
    else:
        logging.info('Parsing '+args.file)
    
    outputfileName=args.file+'.zenith.txt'
    
    if args.out:
        outputfileName=args.out
        
    logging.info('Output will be '+outputfileName)
    
    if writeSettlementFile(args.file, outputfileName, args.aggid)==0:
        if transferSettlementFile(outputfileName)==1:
            return 1
    else:
        return 1
    return 0
