import json, pathlib, sys
import jwt
from dotenv import load_dotenv

OLD_REPO = pathlib.Path("/Users/saras/Licensing/analytics_pipeline")   # <-- adjust
NEW_REPO = pathlib.Path(__file__).resolve().parents[1]

load_dotenv(OLD_REPO / ".env")   # jwt_secret_here, SUB, AUD, HERE_ISS, ...
load_dotenv(NEW_REPO / ".env")   # HERE_* vars
sys.path[:0] = [str(OLD_REPO), str(NEW_REPO / "dags")]

from extract.auth import build_token as old_build, get_headers as old_headers
from here_gql.client import build_token as new_build, build_headers as new_headers, config_from_env

old_tok = old_build("here")
new_tok = new_build(config_from_env())

def show(label, tok, headers):
    print(f"\n=== {label} ===")
    print("jwt header :", jwt.get_unverified_header(tok))
    print("payload    :", json.dumps(jwt.decode(tok, options={"verify_signature": False}), indent=2, default=str))
    print("http hdrs  :", sorted(headers))

show("EXISTING (works)", old_tok, old_headers(old_tok))
show("NEW (400)", new_tok, new_headers(config_from_env()))