from pathlib import Path

from bar import Bar
from bson.json_util import dumps
from clrescue.client import CloudAppClient


client = CloudAppClient()


def main():
	with Bar("Iterating...", count=1) as bar:
		for drop in client:
			if isinstance(drop, dict):
				Path(f'broken--{drop["item_type"]}--{drop["id"]}--{drop["name"]}.info.json').write_text(dumps(drop, indent=4, sort_keys=True))
				bar.step()
				continue
			
			bar.subject = f"{drop.type:8} {drop.uploaded.isoformat()} {drop.slug:12}"
			bar.count = drop.total
			bar.update_bar()
			drop.save()
			bar.step()


if __name__ == '__main__':
	try:
		main()
	except:
		__import__('pudb').post_mortem()
