import os
import tempfile
import shutil
from unittest import TestCase, mock

_marker = object()

class Test_main(TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.hooks = os.path.join(self.tmpdir, 'hooks')
        os.makedirs(self.hooks)
        self.config = os.path.join(self.tmpdir, 'config')
        os.makedirs(self.config)
        self._run_one_hook_patcher = mock.patch('zgres.apply._run_one_hook')
        self.run_one_hook = self._run_one_hook_patcher.start()
        self.hook_results = []
        self.run_one_hook.side_effect = self.pop_hook_result

    def pop_hook_result(self, hook, path):
        res = self.hook_results.pop(0)
        if isinstance(res, Exception):
            raise res
        return res

    def tearDown(self):
        assert not self.hook_results
        self._run_one_hook_patcher.stop()
        shutil.rmtree(self.tmpdir)

    def apply(self):
        from zgres.apply import _apply
        return _apply(self.tmpdir)

    def make_hook(self, filename='10-myhook', for_config='config.json', executable=True):
        file = os.path.join(self.hooks, filename)
        assert not os.path.exists(file)
        with open(file, 'w') as f:
            f.write('ignored, use mock to simulate running this file')
        if executable:
            os.chmod(file, 0o700)
        return file

    def make_config(self, filename='config.json', data='{"MyKey": "MyValue"}'):
        file = os.path.join(self.config, filename)
        with open(file, 'w') as f:
            f.write(data)
        return file

    def test_apply(self):
        self.make_config()
        retcode = self.apply()
        self.assertEqual(retcode, 0)

    def test_config(self):
        from zgres.apply import Config
        self.make_config(filename='myconfig.json', data='{"MyKey": "MyValue"}')
        proxy = Config(self.config)
        self.assertEqual(proxy.get('somename'), None)
        self.assertEqual(proxy['myconfig.json'], {"MyKey": "MyValue"})

    def test_a_hook(self):
        config = self.make_config()
        hookfile = self.make_hook()
        self.hook_results.append(0)
        self.apply()
        self.run_one_hook.assert_called_once_with(hookfile, self.config)
    
    def test_ignore_hidden_hook(self):
        config = self.make_config()
        hookfile = self.make_hook()
        hidden_hook = self.make_hook(filename='.myhook')
        self.hook_results.append(0)
        self.apply()
        self.run_one_hook.assert_called_once_with(hookfile, self.config)

    def test_a_hook_order(self):
        config = self.make_config()
        hook1 = self.make_hook(filename='1hook')
        hook3 = self.make_hook(filename='3hook')
        hook2 = self.make_hook(filename='2hook')
        hook10 = self.make_hook(filename='10hook')
        self.hook_results.extend([0, 0, 0, 0])
        self.apply()
        self.assertEqual(
                self.run_one_hook.call_args_list,
                [mock.call(hook10, self.config),
                    mock.call(hook1, self.config),
                    mock.call(hook2, self.config),
                    mock.call(hook3, self.config),
                    ])

    def test_hook_failure(self):
        config = self.make_config()
        hook1 = self.make_hook(filename='1hook')
        hook2 = self.make_hook(filename='2hook')
        hook3 = self.make_hook(filename='3hook')
        self.hook_results.extend([0, 1, 0]) # second hook fails
        # the process must return a non-zero exit code
        self.assertEqual(self.apply(), 1)
        # and only the first 2 hooks were run
        self.assertEqual(
                self.run_one_hook.call_args_list,
                [mock.call(hook1, self.config),
                    mock.call(hook2, self.config),
                    mock.call(hook3, self.config),
                    ])

    def test_non_executable_hook(self):
        config = self.make_config()
        hook1 = self.make_hook(filename='1hook')
        hook2 = self.make_hook(filename='2hook', executable=False)
        hook3 = self.make_hook(filename='3hook')
        self.hook_results.extend([0, 0])
        self.apply()
        # 2hook is skipped
        self.assertEqual(
                self.run_one_hook.call_args_list,
                [mock.call(hook1, self.config),
                    mock.call(hook3, self.config),
                    ])
