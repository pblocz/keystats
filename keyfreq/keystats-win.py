import os
import keyboard
import sys

keypresses = 0
KEYPRESS_FLUSH_LIMIT = 100

# Initialization
def main():
	global log_file, opened_file
	log_filename = 'keypress_freq.json'

	# Get log filename from command line
	if len(sys.argv) > 1:
		log_filename = sys.argv[1]
	log_file = os.path.expanduser(log_filename)

	# Create log file if not exist
	opened_file = open(log_file, "a")

	# create a hook manager object
	keyboard.on_press(OnKeyPress)

	try:
		keyboard.wait()
	except KeyboardInterrupt:
		# User cancelled from command line.
		pass
	except Exception as ex:
		# Write exceptions to the log file, for analysis later.
		print('Error while catching events:\n {}'.format(ex))
	finally:
		opened_file.close()


def OnKeyPress(event):

	print(event.to_json())
	write_log(event)

# Write stats to file
def write_log(event):
	global opened_file
	global keypresses

	keypresses += 1
	if keypresses > KEYPRESS_FLUSH_LIMIT:
		keypresses = 0
		opened_file.flush()

	opened_file.write(event.to_json() + "\n")

if __name__ == "__main__":
	main()
